use std::fmt::Debug;

use async_trait::async_trait;
use futures::sink::{Sink, SinkExt};
use pgwire::api::auth::{
    finish_authentication, protocol_negotiation, save_startup_parameters_to_metadata,
    DefaultServerParameterProvider, StartupHandler,
};
use pgwire::api::{ClientInfo, PgWireConnectionState, METADATA_USER};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::startup::Authentication;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use tracing::{debug, warn};

use identity::Identity;

pub(super) const IDENTITY_DID_KEY: &str = "identity_did";

/// DID+JWT authentication handler for the PG wire protocol.
///
/// Username = DID string (e.g. `did:key:z6Mk...`), Password = signed JWT.
/// If username is empty or "anonymous", authentication is skipped.
pub struct DIDAuthHandler {
    audience: String,
}

impl DIDAuthHandler {
    pub fn new(audience: String) -> Self {
        Self { audience }
    }

    fn is_anonymous(user: Option<&str>) -> bool {
        match user {
            None => true,
            Some(u) => u.is_empty() || u == "anonymous",
        }
    }
}

#[async_trait]
impl StartupHandler for DIDAuthHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match message {
            PgWireFrontendMessage::Startup(ref startup) => {
                protocol_negotiation(client, startup).await?;
                save_startup_parameters_to_metadata(client, startup);

                let user = client.metadata().get(METADATA_USER).cloned();
                if Self::is_anonymous(user.as_deref()) {
                    debug!("Anonymous PG connection, skipping auth");
                    finish_authentication(client, &DefaultServerParameterProvider::default())
                        .await?;
                } else {
                    debug!(
                        user = user.as_deref(),
                        "DID user connecting, requesting JWT"
                    );
                    client.set_state(PgWireConnectionState::AuthenticationInProgress);
                    client
                        .send(PgWireBackendMessage::Authentication(
                            Authentication::CleartextPassword,
                        ))
                        .await?;
                }
            }
            PgWireFrontendMessage::PasswordMessageFamily(pwd) => {
                let pwd = pwd.into_password()?;
                let jwt_str = &pwd.password;
                let user = client
                    .metadata()
                    .get(METADATA_USER)
                    .cloned()
                    .unwrap_or_default();

                let token_identity = identity::from_token(jwt_str.as_bytes()).map_err(|e| {
                    warn!(error = %e, user = %user, "JWT token parsing failed");
                    PgWireError::InvalidPassword(user.clone())
                })?;

                identity::verify_auth_token(&token_identity, &self.audience).map_err(|e| {
                    warn!(error = %e, user = %user, "JWT token verification failed");
                    PgWireError::InvalidPassword(user.clone())
                })?;

                let token_did = token_identity.did().map_err(|e| {
                    warn!(error = %e, user = %user, "DID extraction from JWT failed");
                    PgWireError::InvalidPassword(user.clone())
                })?;

                if token_did.as_str() != user {
                    warn!(
                        jwt_did = token_did.as_str(),
                        username_did = %user,
                        "JWT issuer DID does not match provided username"
                    );
                    return Err(PgWireError::InvalidPassword(user));
                }

                debug!(did = token_did.as_str(), "PG auth successful");
                client
                    .metadata_mut()
                    .insert(IDENTITY_DID_KEY.to_string(), token_did.as_str().to_string());

                finish_authentication(client, &DefaultServerParameterProvider::default()).await?;
            }
            _ => {}
        }
        Ok(())
    }
}

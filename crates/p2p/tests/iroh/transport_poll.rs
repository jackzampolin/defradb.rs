use super::*;
use futures::poll;
use tokio::time::{advance, timeout, Instant};

fn transport() -> (IrohTransport, mpsc::Receiver<IrohCommand>) {
    let (tx, rx) = mpsc::channel(1);
    (IrohTransport::new(tx, SecretKey::generate()), rx)
}

fn take_reply(commands: &mut mpsc::Receiver<IrohCommand>) -> oneshot::Sender<Result<Vec<PeerId>>> {
    match commands.try_recv().expect("expected a peer-list request") {
        IrohCommand::ConnectedPeers { reply } => reply,
        _ => panic!("unexpected endpoint command"),
    }
}

#[tokio::test(start_paused = true)]
async fn poll_succeeds_when_peer_connects() {
    let (transport, mut commands) = transport();
    let peer = PeerId::new("remote".into());
    let waiting = transport.poll_until_connected(&peer, Duration::from_secs(5));
    tokio::pin!(waiting);

    assert!(poll!(&mut waiting).is_pending());
    take_reply(&mut commands).send(Ok(vec![])).unwrap();
    assert!(poll!(&mut waiting).is_pending());
    advance(Duration::from_millis(50)).await;
    assert!(poll!(&mut waiting).is_pending());
    take_reply(&mut commands)
        .send(Ok(vec![peer.clone()]))
        .unwrap();

    waiting.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn poll_preserves_peer_listing_errors() {
    let (transport, mut commands) = transport();
    let peer = PeerId::new("remote".into());
    let waiting = transport.poll_until_connected(&peer, Duration::from_secs(5));
    tokio::pin!(waiting);

    assert!(poll!(&mut waiting).is_pending());
    take_reply(&mut commands)
        .send(Err(Error::Transport("unavailable".into())))
        .unwrap();

    assert!(matches!(waiting.await, Err(Error::Transport(message)) if message == "unavailable"));
}

#[tokio::test(start_paused = true)]
async fn poll_deadline_bounds_a_pending_reply() {
    let (transport, mut commands) = transport();
    let peer = PeerId::new("remote".into());
    let waiting = transport.poll_until_connected(&peer, Duration::from_millis(10));
    tokio::pin!(waiting);

    assert!(poll!(&mut waiting).is_pending());
    let _pending_reply = take_reply(&mut commands);
    let result = timeout(Duration::from_millis(20), waiting)
        .await
        .expect("peer-list reply outlived the connection deadline");

    assert!(matches!(result, Err(Error::ConnectionTimeout(id)) if id == peer.as_str()));
}

#[tokio::test(start_paused = true)]
async fn poll_deadline_bounds_a_full_command_queue() {
    let (transport, _commands) = transport();
    let (reply, _response) = oneshot::channel();
    assert!(transport
        .command_tx
        .try_send(IrohCommand::ConnectedPeers { reply })
        .is_ok());
    let peer = PeerId::new("remote".into());
    let result = timeout(
        Duration::from_millis(20),
        transport.poll_until_connected(&peer, Duration::from_millis(10)),
    )
    .await
    .expect("command queue outlived the connection deadline");

    assert!(matches!(result, Err(Error::ConnectionTimeout(id)) if id == peer.as_str()));
}

#[tokio::test(start_paused = true)]
async fn poll_deadline_can_be_shorter_than_the_poll_interval() {
    let (transport, mut commands) = transport();
    let peer = PeerId::new("remote".into());
    let start = Instant::now();
    let waiting = transport.poll_until_connected(&peer, Duration::from_millis(10));
    tokio::pin!(waiting);

    assert!(poll!(&mut waiting).is_pending());
    take_reply(&mut commands).send(Ok(vec![])).unwrap();
    let result = timeout(Duration::from_millis(20), waiting)
        .await
        .expect("poll interval outlived the connection deadline");

    assert!(matches!(result, Err(Error::ConnectionTimeout(_))));
    assert_eq!(start.elapsed(), Duration::from_millis(10));
}

#[tokio::test(start_paused = true)]
async fn poll_stops_on_channel_closure_during_sleep() {
    let (transport, mut commands) = transport();
    let peer = PeerId::new("remote".into());
    let waiting = transport.poll_until_connected(&peer, Duration::from_secs(5));
    tokio::pin!(waiting);

    assert!(poll!(&mut waiting).is_pending());
    take_reply(&mut commands).send(Ok(vec![])).unwrap();
    assert!(poll!(&mut waiting).is_pending());
    commands.close();

    assert!(matches!(
        poll!(&mut waiting),
        std::task::Poll::Ready(Err(Error::ChannelSend))
    ));
}

#[tokio::test(start_paused = true)]
async fn poll_stops_on_channel_closure_with_a_pending_reply() {
    let (transport, mut commands) = transport();
    let peer = PeerId::new("remote".into());
    let waiting = transport.poll_until_connected(&peer, Duration::from_secs(5));
    tokio::pin!(waiting);

    assert!(poll!(&mut waiting).is_pending());
    let _pending_reply = take_reply(&mut commands);
    commands.close();

    assert!(matches!(
        poll!(&mut waiting),
        std::task::Poll::Ready(Err(Error::ChannelSend))
    ));
}

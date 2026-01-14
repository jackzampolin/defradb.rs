//! IPLD traversal helpers and visitor pattern.

use libipld::Ipld;

use super::cid_convert::cid_from_libipld;
use crate::block::Block;
use crate::Result;

/// Extract all CID links from an IPLD value recursively.
///
/// Traverses the IPLD tree depth-first and collects all Link values found.
/// Links are returned in traversal order (not sorted).
pub fn extract_links(ipld: &Ipld) -> Result<Vec<cid::Cid>> {
    let mut links = Vec::new();
    extract_links_recursive(ipld, &mut links)?;
    Ok(links)
}

fn extract_links_recursive(ipld: &Ipld, links: &mut Vec<cid::Cid>) -> Result<()> {
    match ipld {
        Ipld::Link(cid) => {
            links.push(cid_from_libipld(cid)?);
        }
        Ipld::List(items) => {
            for item in items {
                extract_links_recursive(item, links)?;
            }
        }
        Ipld::Map(map) => {
            for value in map.values() {
                extract_links_recursive(value, links)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Visitor trait for traversing IPLD structures.
///
/// Implement this trait to perform custom operations while walking an IPLD DAG.
///
/// # Example
/// ```ignore
/// struct LinkCounter { count: usize }
/// impl IpldVisitor for LinkCounter {
///     fn visit(&mut self, _ipld: &Ipld) -> bool { true }
///     fn visit_link(&mut self, _cid: &cid::Cid) { self.count += 1; }
/// }
/// ```
pub trait IpldVisitor {
    /// Called for each IPLD value encountered during traversal.
    ///
    /// Return `true` to continue traversal into children, `false` to skip children.
    fn visit(&mut self, ipld: &Ipld) -> bool;

    /// Called when a link is encountered.
    ///
    /// Override this to handle link resolution (e.g., fetch linked blocks from storage).
    fn visit_link(&mut self, _cid: &cid::Cid) {
        // Default: do nothing
    }
}

/// Walk an IPLD tree with a visitor.
///
/// Calls visitor methods for each node in the tree. If the visitor's `visit`
/// method returns `false`, children of that node are skipped.
pub fn walk_ipld<V: IpldVisitor>(ipld: &Ipld, visitor: &mut V) -> Result<()> {
    if !visitor.visit(ipld) {
        return Ok(());
    }

    match ipld {
        Ipld::Link(cid) => {
            visitor.visit_link(&cid_from_libipld(cid)?);
        }
        Ipld::List(items) => {
            for item in items {
                walk_ipld(item, visitor)?;
            }
        }
        Ipld::Map(map) => {
            for value in map.values() {
                walk_ipld(value, visitor)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Collect all links from a Block by walking its IPLD representation.
///
/// Unlike `Block::all_links()` which only returns heads and named links,
/// this traverses the entire IPLD tree and may find links in nested structures.
pub fn collect_block_links(block: &Block) -> Result<Vec<cid::Cid>> {
    let ipld = Ipld::try_from(block)?;
    extract_links(&ipld)
}

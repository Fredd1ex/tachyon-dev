//! Frame invalidation separates changed layout work from visible-row painting.
#[derive(PartialEq, Eq)]
pub(super) struct LayoutKey {
    pub(super) width: u16,
    pub(super) revision: u64,
    pub(super) structure: u64,
    pub(super) trace: Option<usize>,
    pub(super) worker: Option<(usize, String)>,
    pub(super) busy: bool,
    pub(super) activity: String,
    pub(super) workers: Vec<(String, u64)>,
}

pub(in crate::app) mod projection;

pub(in crate::app) mod layout;

pub(in crate::app) mod trace;

pub(in crate::app) mod text;

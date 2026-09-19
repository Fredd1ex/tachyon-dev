//! Cached turn projections, wrapped layouts, and viewport height index.
use super::{build_turn_cells, cell_key, elapsed, transcript::LayoutKey, Hit, Thread};
use ratatui::text::Line;
use std::collections::{hash_map::Entry, HashMap, HashSet};

#[derive(Clone, Default)]
pub(super) struct TranscriptView {
    pub(super) attention_hits: Vec<Hit>,
    pub(super) total_height: usize,
    pub(super) viewport: usize,
    pub(super) turns: usize,
    pub(super) anchor_turn: Option<usize>,
    pub(super) starts: Vec<usize>,
    pub(super) heights: Vec<usize>,
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub(super) struct CellKey {
    pub(super) prompt_timestamp: u64,
    pub(super) prompt_index: usize,
}

pub(super) struct CachedLayout {
    pub(super) revision: CellRevision,
    pub(super) variant: u8,
    pub(super) layout: CellLayout,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct CellRevision {
    pub(super) item: u64,
    pub(super) metric: u64,
    pub(super) worker: u64,
}

#[derive(Clone, Default)]
pub(super) struct CellLayout {
    pub(super) activity_row: Option<usize>,
    pub(super) lines: Vec<Line<'static>>,
    pub(super) hits: Vec<Hit>,
    pub(super) timers: Vec<elapsed::Badge>,
}

#[derive(Default)]
pub(super) struct TurnLayoutCache {
    pub(super) layouts: HashMap<CellKey, CachedLayout>,
    pub(super) width: Option<u16>,
    // Retain only the previous width, not every width seen during a resize drag.
    spare: Option<(u16, HashMap<CellKey, CachedLayout>)>,
    pub(super) structure_revision: Option<u64>,
    pub(super) frame_key: Option<LayoutKey>,
    pub(super) order: Vec<(CellKey, Option<String>)>,
    #[cfg(test)]
    pub(super) height_passes: usize,
    #[cfg(test)]
    pub(super) builds: usize,
}

impl TurnLayoutCache {
    pub(super) fn reset(&mut self) {
        self.layouts.clear();
        self.width = None;
        self.spare = None;
        self.structure_revision = None;
        self.frame_key = None;
        self.order.clear();
    }

    pub(super) fn prepare(&mut self, width: u16, cells: &[TurnCell], structure_revision: u64) {
        if self.width != Some(width) {
            let next = self
                .spare
                .take()
                .filter(|(cached_width, _)| *cached_width == width)
                .map(|(_, layouts)| layouts)
                .unwrap_or_default();
            let previous = std::mem::replace(&mut self.layouts, next);
            self.spare = self.width.map(|width| (width, previous));
            self.frame_key = None;
            self.structure_revision = None;
            self.width = Some(width);
        }
        if self.structure_revision == Some(structure_revision) {
            return;
        }
        let valid = cells.iter().map(cell_key).collect::<HashSet<_>>();
        self.layouts.retain(|key, _| valid.contains(key));
        if let Some((_, layouts)) = &mut self.spare {
            layouts.retain(|key, _| valid.contains(key));
        }
        self.structure_revision = Some(structure_revision);
    }

    pub(super) fn layout<F>(
        &mut self,
        key: CellKey,
        revision: CellRevision,
        variant: u8,
        build: F,
    ) -> &CellLayout
    where
        F: FnOnce() -> CellLayout,
    {
        let entry = self.layouts.entry(key);
        let cached = match entry {
            Entry::Occupied(entry) => {
                let cached = entry.into_mut();
                if cached.revision == revision && cached.variant == variant {
                    return &cached.layout;
                }
                *cached = CachedLayout {
                    revision,
                    variant,
                    layout: build(),
                };
                cached
            }
            Entry::Vacant(entry) => entry.insert(CachedLayout {
                revision,
                variant,
                layout: build(),
            }),
        };
        #[cfg(test)]
        {
            self.builds += 1;
        }
        &cached.layout
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn width_cache_is_bounded_pruned_and_reset() {
        let mut cache = TurnLayoutCache::default();
        let cells = [TurnCell {
            prompt: 0,
            items: vec![0],
            prompt_timestamp: 1,
        }];
        let revision = CellRevision {
            item: 1,
            metric: 0,
            worker: 0,
        };
        for width in 10..100 {
            cache.prepare(width, &cells, 1);
            cache.layout(cell_key(&cells[0]), revision, 0, CellLayout::default);
            assert_eq!(cache.layouts.len(), 1);
            assert!(cache
                .spare
                .as_ref()
                .is_none_or(|(_, layouts)| layouts.len() <= 1));
        }
        cache.prepare(100, &[], 2);
        assert!(cache.layouts.is_empty());
        assert!(cache.spare.as_ref().unwrap().1.is_empty());
        cache.reset();
        assert!(cache.spare.is_none());
        assert!(cache.width.is_none());
    }
}

pub(super) struct TurnCell {
    pub(super) prompt: usize,
    pub(super) items: Vec<usize>,
    pub(super) prompt_timestamp: u64,
}

#[derive(Default)]
pub(super) struct TurnProjection {
    pub(super) structure_revision: Option<u64>,
    pub(super) cells: Vec<TurnCell>,
}

impl TurnProjection {
    pub(super) fn reset(&mut self) {
        self.structure_revision = None;
        self.cells.clear();
    }

    pub(super) fn update(&mut self, thread: &Thread) -> bool {
        if self.structure_revision == Some(thread.structure_revision) {
            return false;
        }
        self.cells = build_turn_cells(thread);
        self.structure_revision = Some(thread.structure_revision);
        true
    }
}

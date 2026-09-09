#![forbid(unsafe_code)]

//! Trusted, compiled-in packages of existing runtime tools, not a plugin loader.
//! Manifests describe installed operations; they do not grant policy permissions.

use std::sync::Arc;

use super::manifest::{Manifest, Package};
use super::packages::Packages;
use crate::harness::backend::Local;
use crate::harness::runtime::{
    AgentBrowserTool, ArtifactTool, BrowserAvailability, EditTool, ExecTool, FindTool, GrepTool,
    IpythonTool, LsTool, ReadTool, WriteTool,
};
use crate::harness::tools;

pub fn native() -> Packages {
    let mut packages = Packages::default();
    for package in [workspace(), exec(), artifact()] {
        packages.register(package).expect("valid built-in package");
    }
    packages
}

pub fn workspace() -> Package {
    Package {
        manifest: Manifest {
            name: "workspace",
            version: env!("CARGO_PKG_VERSION"),
            description: "Workspace file inspection, search, and editing.",
            usage: tools::workspace::USAGE,
            operations: &["read", "write", "edit", "ls", "find", "grep"],
        },
        tools: vec![
            Arc::new(ReadTool::new()),
            Arc::new(WriteTool::new()),
            Arc::new(EditTool::new()),
            Arc::new(LsTool::new()),
            Arc::new(FindTool::new()),
            Arc::new(GrepTool::new()),
        ],
    }
}

pub fn exec() -> Package {
    Package {
        manifest: Manifest {
            name: "exec",
            version: env!("CARGO_PKG_VERSION"),
            description: "Workspace process execution.",
            usage: tools::exec::USAGE,
            operations: &["exec"],
        },
        tools: vec![Arc::new(ExecTool::new())],
    }
}

pub fn artifact() -> Package {
    Package {
        manifest: Manifest {
            name: "artifact",
            version: env!("CARGO_PKG_VERSION"),
            description: "Workspace artifact registration.",
            usage: tools::artifact::USAGE,
            operations: &["artifact"],
        },
        tools: vec![Arc::new(ArtifactTool::new())],
    }
}

pub fn ipython(backend: Arc<Local>) -> Package {
    Package {
        manifest: Manifest {
            name: "ipython",
            version: env!("CARGO_PKG_VERSION"),
            description: "Persistent workspace Python execution.",
            usage: tools::python::USAGE,
            operations: &["ipython"],
        },
        tools: vec![Arc::new(IpythonTool::new(backend))],
    }
}

/// Unavailable browsers contribute neither a manifest nor an operation.
pub fn browser(backend: Arc<Local>, availability: BrowserAvailability) -> Option<Package> {
    if !matches!(&availability, BrowserAvailability::Available) {
        return None;
    }
    Some(Package {
        manifest: Manifest {
            name: "browser",
            version: env!("CARGO_PKG_VERSION"),
            description: "Read and interact with the configured browser.",
            usage: tools::browser::USAGE,
            operations: &["agent_browser"],
        },
        tools: vec![Arc::new(AgentBrowserTool::new(backend, availability))],
    })
}

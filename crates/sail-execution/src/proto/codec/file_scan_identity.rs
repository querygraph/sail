use std::sync::Arc;

use datafusion::common::{Result, plan_err};
use datafusion::datasource::physical_plan::{FileScanConfig, FileSource};
use datafusion_proto::physical_plan::from_proto::parse_protobuf_file_scan_config;
use datafusion_proto::physical_plan::{PhysicalPlanDecodeContext, PhysicalProtoConverterExtension};
use datafusion_proto::protobuf::FileScanExecConf;

use crate::plan::r#gen::{FileGroupIdentity, FileObjectIdentity, FileScanIdentity};

/// Preserve fields omitted by DataFusion's shared file scan protobuf.
pub(super) fn encode_file_scan_identity(config: &FileScanConfig) -> FileScanIdentity {
    FileScanIdentity {
        groups: config
            .file_groups
            .iter()
            .map(|group| FileGroupIdentity {
                files: group
                    .iter()
                    .map(|file| FileObjectIdentity {
                        location: file.object_meta.location.as_ref().to_owned(),
                        e_tag: file.object_meta.e_tag.clone(),
                        version: file.object_meta.version.clone(),
                    })
                    .collect(),
            })
            .collect(),
    }
}

/// Restore identity before the caller applies format-specific scan properties.
///
/// A missing supplement retains the behavior of plans encoded by older Sail
/// versions. Identity usability and conditional reads remain the file source's
/// responsibility; transport preserves optional strings without normalizing them.
pub(super) fn parse_file_scan_config_with_identity(
    proto: &FileScanExecConf,
    ctx: &PhysicalPlanDecodeContext<'_>,
    proto_converter: &dyn PhysicalProtoConverterExtension,
    file_source: Arc<dyn FileSource>,
    identity: Option<&FileScanIdentity>,
) -> Result<FileScanConfig> {
    let mut config = parse_protobuf_file_scan_config(proto, ctx, proto_converter, file_source)?;
    let Some(identity) = identity else {
        return Ok(config);
    };
    if identity.groups.len() != config.file_groups.len() {
        return plan_err!(
            "File scan identity group count {} does not match scan group count {}",
            identity.groups.len(),
            config.file_groups.len()
        );
    }
    for (group_index, (group, identities)) in config
        .file_groups
        .iter_mut()
        .zip(&identity.groups)
        .enumerate()
    {
        if identities.files.len() != group.len() {
            return plan_err!(
                "File scan identity file count {} does not match group {group_index} file count {}",
                identities.files.len(),
                group.len()
            );
        }
        for (file_index, identity) in identities.files.iter().enumerate() {
            let meta = &mut group[file_index].object_meta;
            if identity.location != meta.location.as_ref() {
                return plan_err!(
                    "File scan identity location '{}' does not match group {group_index} file \
                     {file_index} location '{}'",
                    identity.location,
                    meta.location
                );
            }
            meta.e_tag = identity.e_tag.clone();
            meta.version = identity.version.clone();
        }
    }
    Ok(config)
}

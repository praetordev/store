package store

// Exported column lists for the resource tables, referenced in place of `SELECT *`.
// Centralizing them here means a new DB column can't silently break a scan
// ("missing destination name X") or change an API response, and the single
// reflection test (columns_test.go) guards every list against its struct's db tags
// — so drift is caught at test time, not in production.
//
// HostCols/GroupCols/ScheduleCols are also read on the dispatch path (the scheduler
// tick and pkg/inventoryrender, which ingestion runs at every job dispatch). Those
// readers now go through this store's methods (or reference these consts directly),
// so the lists live in exactly one place instead of being duplicated in pkg/models
// (#91).
//
// Each list is the exact set of db tags on the scanned struct. Keep them in sync;
// the test will fail loudly if they diverge.
const (
	CredentialCols     = `id, organization_id, credential_type_id, name, description, inputs, created_at, modified_at`
	CredentialTypeCols = `id, name, description, inputs, injectors, managed, created_at, modified_at`
	HostCols           = `id, inventory_id, name, description, variables, enabled, is_control_node, is_runner_host, runner_last_seen, runner_healthy, created_at, modified_at`
	InventoryCols      = `id, organization_id, name, description, kind, content, created_at, modified_at`
	GroupCols          = `id, inventory_id, name, description, variables, created_at, modified_at`
	JobTemplateCols    = `id, organization_id, name, description, inventory_id, project_id, playbook, playbook_content, unified_job_template_id, credential_id, execution_pack_id, forks, job_type, verbosity, extra_vars, job_limit, ask_variables_on_launch, ask_limit_on_launch, ask_inventory_on_launch, ask_credential_on_launch, survey_enabled, survey_spec, webhook_enabled, webhook_service, webhook_key, use_fact_cache, allow_simultaneous, created_at, modified_at`
	ProjectCols        = `id, organization_id, name, description, scm_type, scm_url, scm_branch, created_at, modified_at`
	OrganizationCols   = `id, name, description, created_at, modified_at`
	TeamCols           = `id, organization_id, name, description, created_at, modified_at`
	ScheduleCols       = `id, name, description, unified_job_template_id, workflow_template_id, inventory_source_id, actor_user_id, rrule, next_run, enabled, extra_vars, created_at, modified_at`
)

// Prefixed qualifies a comma-separated column list with a table alias, e.g.
// Prefixed("uj", "id, name") -> "uj.id, uj.name". Exported for handlers that
// join and must disambiguate columns.
func Prefixed(alias, cols string) string { return prefixed(alias, cols) }

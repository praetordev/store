package store

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/praetordev/models"
)

// TemplateStore is the data-access layer for the job-templates domain.
type TemplateStore struct {
	db *sqlx.DB
}

func NewTemplateStore(db *sqlx.DB) *TemplateStore { return &TemplateStore{db: db} }

// ListAll returns a page of all job templates (superuser/auditor view).
func (s *TemplateStore) ListAll(ctx context.Context, limit, offset int) ([]models.JobTemplate, error) {
	templates := []models.JobTemplate{}
	err := s.db.SelectContext(ctx, &templates,
		`SELECT `+JobTemplateCols+` FROM job_templates ORDER BY id DESC LIMIT $1 OFFSET $2`, limit, offset)
	return templates, wrap("TemplateStore.ListAll", err)
}

// CountAll returns the total number of job templates.
func (s *TemplateStore) CountAll(ctx context.Context) (int64, error) {
	var total int64
	err := s.db.GetContext(ctx, &total, "SELECT count(*) FROM job_templates")
	return total, wrap("TemplateStore.CountAll", err)
}

// ListByIDs returns a page of the job templates whose id is in ids.
func (s *TemplateStore) ListByIDs(ctx context.Context, ids []int64, limit, offset int) ([]models.JobTemplate, error) {
	templates := []models.JobTemplate{}
	if len(ids) == 0 {
		return templates, nil
	}
	q, args, err := sqlx.In(`SELECT `+JobTemplateCols+` FROM job_templates WHERE id IN (?) ORDER BY id DESC LIMIT ? OFFSET ?`, ids, limit, offset)
	if err != nil {
		return nil, wrap("TemplateStore.ListByIDs", err)
	}
	q = s.db.Rebind(q)
	err = s.db.SelectContext(ctx, &templates, q, args...)
	return templates, wrap("TemplateStore.ListByIDs", err)
}

// Get returns a single job template by id.
func (s *TemplateStore) Get(ctx context.Context, id int64) (models.JobTemplate, error) {
	var t models.JobTemplate
	err := s.db.GetContext(ctx, &t, `SELECT `+JobTemplateCols+` FROM job_templates WHERE id = $1`, id)
	return t, wrap("TemplateStore.Get", err)
}

// Create inserts a job template and its backing unified_job_templates row in one
// transaction, returning the persisted template. input.UnifiedJobTemplateID is
// ignored (assigned here). Caller is responsible for validation + RBAC.
func (s *TemplateStore) Create(ctx context.Context, input models.JobTemplate) (models.JobTemplate, error) {
	var created models.JobTemplate
	tx, err := s.db.Beginx()
	if err != nil {
		return created, wrap("TemplateStore.Create", err)
	}
	defer tx.Rollback()

	// 1. Insert into unified_job_templates to get ID
	var ujtID int64
	if err := tx.QueryRowxContext(ctx, "INSERT INTO unified_job_templates (name) VALUES ($1) RETURNING id", input.Name).Scan(&ujtID); err != nil {
		return created, wrap("TemplateStore.Create", err)
	}

	// 2. Insert into job_templates
	query := `
		INSERT INTO job_templates (organization_id, name, description, playbook, playbook_content, project_id, inventory_id, job_type, verbosity, unified_job_template_id, credential_id, extra_vars, job_limit, ask_variables_on_launch, ask_limit_on_launch, ask_inventory_on_launch, ask_credential_on_launch, survey_enabled, survey_spec, webhook_enabled, webhook_service, webhook_key, use_fact_cache, execution_pack_id, allow_simultaneous)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25)
		RETURNING ` + JobTemplateCols
	if err := tx.QueryRowxContext(ctx, query,
		input.OrganizationID, input.Name, input.Description,
		input.Playbook, input.PlaybookContent, input.ProjectID, input.InventoryID,
		input.JobType, input.Verbosity, ujtID, input.CredentialID,
		input.ExtraVars, input.JobLimit, input.AskVariablesOnLaunch, input.AskLimitOnLaunch,
		input.AskInventoryOnLaunch, input.AskCredentialOnLaunch,
		input.SurveyEnabled, input.SurveySpec,
		input.WebhookEnabled, input.WebhookService, input.WebhookKey, input.UseFactCache,
		input.ExecutionPackID, input.AllowSimultaneous,
	).StructScan(&created); err != nil {
		return created, wrap("TemplateStore.Create", err)
	}
	if err := tx.Commit(); err != nil {
		return created, wrap("TemplateStore.Create", err)
	}
	return created, nil
}

// Update applies an edit to a job template and returns the persisted row.
func (s *TemplateStore) Update(ctx context.Context, id int64, input models.JobTemplate) (models.JobTemplate, error) {
	query := `
		UPDATE job_templates
		SET name = $2, description = $3, playbook = $4, playbook_content = $5,
		    project_id = $6, verbosity = $7, inventory_id = $8, credential_id = $9,
		    extra_vars = $10, job_limit = $11, ask_variables_on_launch = $12, ask_limit_on_launch = $13,
		    ask_inventory_on_launch = $14, ask_credential_on_launch = $15,
		    survey_enabled = $16, survey_spec = $17,
		    webhook_enabled = $18, webhook_service = $19, webhook_key = $20, use_fact_cache = $21,
		    execution_pack_id = $22, allow_simultaneous = $23,
		    modified_at = now()
		WHERE id = $1
		RETURNING ` + JobTemplateCols
	var updated models.JobTemplate
	err := s.db.QueryRowxContext(ctx, query,
		id, input.Name, input.Description, input.Playbook,
		input.PlaybookContent, input.ProjectID, input.Verbosity, input.InventoryID, input.CredentialID,
		input.ExtraVars, input.JobLimit, input.AskVariablesOnLaunch, input.AskLimitOnLaunch,
		input.AskInventoryOnLaunch, input.AskCredentialOnLaunch,
		input.SurveyEnabled, input.SurveySpec,
		input.WebhookEnabled, input.WebhookService, input.WebhookKey, input.UseFactCache,
		input.ExecutionPackID, input.AllowSimultaneous,
	).StructScan(&updated)
	return updated, wrap("TemplateStore.Update", err)
}

// Delete removes a job template by id.
func (s *TemplateStore) Delete(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM job_templates WHERE id = $1`, id)
	return wrap("TemplateStore.Delete", err)
}

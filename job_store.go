// Package store holds the API's data-access layer: SQL lives here, behind small
// interfaces the handlers declare and depend on, so handlers keep only RBAC and
// rendering. Column lists are always explicit (never SELECT *) so a new DB column
// can neither break a scan nor silently change an API response.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/praetordev/launch"
	"github.com/praetordev/models"
)

// Explicit column lists, matching the model structs' db tags (and deliberately
// excluding internal-only columns like unified_jobs.concurrency_key and
// execution_runs.runner_host_id/reconcile_*/credential_id).
const (
	unifiedJobCols   = `id, unified_job_template_id, name, status, current_run_id, created_at, started_at, finished_at, cancel_requested, job_args`
	executionRunCols = `id, unified_job_id, created_at, started_at, finished_at, state, last_heartbeat_at, last_event_seq, persisted_event_seq`
	jobEventCols     = `id, unified_job_id, execution_run_id, seq, event_type, host_id, task_name, play_name, event_data, stdout_snippet, created_at`
)

// JobStore is the data-access layer for the jobs domain.
type JobStore struct {
	db *sqlx.DB
}

func NewJobStore(db *sqlx.DB) *JobStore { return &JobStore{db: db} }

// ListRecent returns the most recent unified jobs (superuser/auditor view).
func (s *JobStore) ListRecent(ctx context.Context, limit int) ([]models.UnifiedJob, error) {
	jobs := []models.UnifiedJob{}
	err := s.db.SelectContext(ctx, &jobs,
		`SELECT `+unifiedJobCols+` FROM unified_jobs ORDER BY created_at DESC LIMIT $1`, limit)
	return jobs, wrap("JobStore.ListRecent", err)
}

// ListReadable returns recent unified jobs whose governing template is in tmplIDs.
func (s *JobStore) ListReadable(ctx context.Context, tmplIDs []int64, limit int) ([]models.UnifiedJob, error) {
	jobs := []models.UnifiedJob{}
	if len(tmplIDs) == 0 {
		return jobs, nil
	}
	q, args, err := sqlx.In(`
		SELECT `+prefixed("uj", unifiedJobCols)+`
		FROM unified_jobs uj
		JOIN job_templates jt ON uj.unified_job_template_id = jt.unified_job_template_id
		WHERE jt.id IN (?)
		ORDER BY uj.created_at DESC LIMIT ?`, tmplIDs, limit)
	if err != nil {
		return nil, wrap("JobStore.ListReadable", err)
	}
	q = s.db.Rebind(q)
	err = s.db.SelectContext(ctx, &jobs, q, args...)
	return jobs, wrap("JobStore.ListReadable", err)
}

// ListReadableByScopes returns recent jobs governed by either an authorized job
// template or an authorized inventory. Inventory ownership is resolved through
// inventory_sync_history, whose inventory_id is populated by the database from
// the inventory source at enqueue time. Callers must supply only IDs already
// authorized for the requesting principal; empty scopes fail closed.
func (s *JobStore) ListReadableByScopes(ctx context.Context, tmplIDs, inventoryIDs []int64, limit int) ([]models.UnifiedJob, error) {
	jobs := []models.UnifiedJob{}
	q, queryArgs, ok, err := readableByScopesQuery(tmplIDs, inventoryIDs, limit)
	if err != nil {
		return nil, wrap("JobStore.ListReadableByScopes", err)
	}
	if !ok {
		return jobs, nil
	}
	q = s.db.Rebind(q)
	err = s.db.SelectContext(ctx, &jobs, q, queryArgs...)
	return jobs, wrap("JobStore.ListReadableByScopes", err)
}

func readableByScopesQuery(tmplIDs, inventoryIDs []int64, limit int) (string, []interface{}, bool, error) {
	clauses := make([]string, 0, 2)
	args := make([]interface{}, 0, 3)
	if len(tmplIDs) > 0 {
		clauses = append(clauses, `EXISTS (
			SELECT 1 FROM job_templates jt
			WHERE jt.unified_job_template_id = uj.unified_job_template_id
			  AND jt.id IN (?)
		)`)
		args = append(args, tmplIDs)
	}
	if len(inventoryIDs) > 0 {
		clauses = append(clauses, `EXISTS (
			SELECT 1 FROM inventory_sync_history ish
			WHERE ish.unified_job_id = uj.id
			  AND ish.inventory_id IN (?)
		)`)
		args = append(args, inventoryIDs)
	}
	if len(clauses) == 0 {
		return "", nil, false, nil
	}
	args = append(args, limit)
	q, queryArgs, err := sqlx.In(`
		SELECT `+prefixed("uj", unifiedJobCols)+`
		FROM unified_jobs uj
		WHERE (`+strings.Join(clauses, " OR ")+`)
		ORDER BY uj.created_at DESC, uj.id DESC
		LIMIT ?`, args...)
	if err != nil {
		return "", nil, false, err
	}
	return q, queryArgs, true, nil
}

// GetRun returns a single execution run by id.
func (s *JobStore) GetRun(ctx context.Context, runID uuid.UUID) (models.ExecutionRun, error) {
	var run models.ExecutionRun
	err := s.db.GetContext(ctx, &run,
		`SELECT `+executionRunCols+` FROM execution_runs WHERE id = $1`, runID)
	return run, wrap("JobStore.GetRun", err)
}

// ListEvents returns a run's job events in sequence order.
func (s *JobStore) ListEvents(ctx context.Context, runID uuid.UUID) ([]models.JobEvent, error) {
	events := []models.JobEvent{}
	err := s.db.SelectContext(ctx, &events,
		`SELECT `+jobEventCols+` FROM job_events WHERE execution_run_id = $1 ORDER BY seq ASC`, runID)
	return events, wrap("JobStore.ListEvents", err)
}

type DiagnosticQuery struct {
	AfterSeq int64
	Limit    int
	Kind     string
	Outcome  string
}

type DiagnosticEvent struct {
	Seq         int64     `db:"seq"`
	EventType   string    `db:"event_type"`
	HostID      *int64    `db:"host_id"`
	TaskName    *string   `db:"task_name"`
	PlayName    *string   `db:"play_name"`
	Outcome     *string   `db:"outcome"`
	Changed     bool      `db:"changed"`
	DurationMS  *int64    `db:"duration_ms"`
	FailureCode *string   `db:"failure_code"`
	CreatedAt   time.Time `db:"created_at"`
}

type DiagnosticSummary struct {
	UnifiedJobID     int64      `db:"unified_job_id"`
	RunState         string     `db:"run_state"`
	LastEventSeq     int64      `db:"last_event_seq"`
	StartedAt        *time.Time `db:"started_at"`
	FinishedAt       *time.Time `db:"finished_at"`
	SourceJobID      *int64     `db:"source_job_id"`
	Attempt          int        `db:"attempt"`
	SubsequentJobIDs []int64
	CurrentPhase     string
	SafeFailureCode  string
}

// ListDiagnostics returns an exclusive, sequence-keyed page. Kind and outcome
// are validated by the handler; the SQL remains bounded and uses the composite
// run/filter/sequence indexes declared with the diagnostics migration.
func (s *JobStore) ListDiagnostics(ctx context.Context, runID uuid.UUID, query DiagnosticQuery) ([]DiagnosticEvent, error) {
	if query.Limit < 1 || query.Limit > 200 {
		return nil, fmt.Errorf("diagnostic limit must be between 1 and 200")
	}
	where := []string{"execution_run_id = $1", "seq > $2"}
	args := []interface{}{runID, query.AfterSeq}
	if query.Outcome != "" {
		args = append(args, query.Outcome)
		where = append(where, fmt.Sprintf("diagnostic_outcome = $%d", len(args)))
	}
	if query.Kind != "" && query.Kind != "all" {
		var eventTypes []string
		switch query.Kind {
		case "lifecycle":
			eventTypes = []string{"JOB_STARTED", "JOB_COMPLETED", "JOB_FAILED", "JOB_CANCELED", "RUNNER_ONLINE", "RESUMED_FROM_CHECKPOINT"}
		case "task":
			eventTypes = []string{"PLAY_STARTED", "TASK_STARTED", "TASK_COMPLETED"}
		case "host":
			eventTypes = []string{"HOST_OK", "HOST_CHANGED", "HOST_FAILED", "HOST_UNREACHABLE", "HOST_SKIPPED"}
		case "failure":
			eventTypes = []string{"JOB_FAILED", "HOST_FAILED", "HOST_UNREACHABLE"}
		default:
			return nil, fmt.Errorf("unsupported diagnostic kind")
		}
		placeholders := make([]string, 0, len(eventTypes))
		for _, eventType := range eventTypes {
			args = append(args, eventType)
			placeholders = append(placeholders, fmt.Sprintf("$%d", len(args)))
		}
		where = append(where, "event_type IN ("+strings.Join(placeholders, ",")+")")
	}
	args = append(args, query.Limit+1)
	statement := `SELECT seq, event_type, host_id, task_name, play_name,
		CASE WHEN event_data->>'outcome' IN ('ok','changed','failed','unreachable','skipped')
			THEN event_data->>'outcome' END AS outcome,
		CASE WHEN event_data->>'changed' IN ('true','false')
			THEN (event_data->>'changed')::boolean ELSE false END AS changed,
		CASE WHEN event_data->>'duration_ms' ~ '^[0-9]+$'
			THEN (event_data->>'duration_ms')::bigint END AS duration_ms,
		CASE WHEN event_data->>'failure_code' IN ('task_failed','host_unreachable','execution_failed','ignored')
			THEN event_data->>'failure_code' END AS failure_code,
		created_at
		FROM job_events WHERE ` + strings.Join(where, " AND ") +
		` ORDER BY seq ASC LIMIT $` + fmt.Sprint(len(args))
	events := []DiagnosticEvent{}
	if err := s.db.SelectContext(ctx, &events, statement, args...); err != nil {
		return nil, wrap("JobStore.ListDiagnostics", err)
	}
	return events, nil
}

func (s *JobStore) DiagnosticSummary(ctx context.Context, runID uuid.UUID) (DiagnosticSummary, error) {
	var summary DiagnosticSummary
	err := s.db.GetContext(ctx, &summary, `
		WITH RECURSIVE lineage AS (
			SELECT uj.id, uj.source_job_id, 1 AS depth
			FROM execution_runs er JOIN unified_jobs uj ON uj.id = er.unified_job_id
			WHERE er.id = $1
			UNION ALL
			SELECT parent.id, parent.source_job_id, lineage.depth + 1
			FROM unified_jobs parent JOIN lineage ON parent.id = lineage.source_job_id
		)
		SELECT er.unified_job_id, er.state AS run_state, er.last_event_seq,
			er.started_at, er.finished_at, uj.source_job_id, MAX(lineage.depth)::int AS attempt
		FROM execution_runs er
		JOIN unified_jobs uj ON uj.id = er.unified_job_id
		JOIN lineage ON true
		WHERE er.id = $1
		GROUP BY er.unified_job_id, er.state, er.last_event_seq, er.started_at,
			er.finished_at, uj.source_job_id`, runID)
	if err != nil {
		return summary, wrap("JobStore.DiagnosticSummary", err)
	}
	_ = s.db.SelectContext(ctx, &summary.SubsequentJobIDs, `
		WITH RECURSIVE descendants AS (
			SELECT id FROM unified_jobs WHERE source_job_id = $1
			UNION ALL
			SELECT child.id FROM unified_jobs child
			JOIN descendants parent ON child.source_job_id = parent.id
		)
		SELECT id FROM descendants ORDER BY id`, summary.UnifiedJobID)

	var eventType string
	var failureCode *string
	_ = s.db.QueryRowContext(ctx, `
		SELECT event_type,
			CASE WHEN event_data->>'failure_code' IN ('task_failed','host_unreachable','execution_failed','ignored')
				THEN event_data->>'failure_code' END
		FROM job_events WHERE execution_run_id=$1 ORDER BY seq DESC LIMIT 1`, runID).Scan(&eventType, &failureCode)
	summary.CurrentPhase, summary.SafeFailureCode = classifyDiagnostic(summary.RunState, eventType, failureCode)
	return summary, nil
}

func classifyDiagnostic(state, eventType string, failureCode *string) (string, string) {
	phase := "queued"
	switch {
	case state == "successful" || state == "failed" || state == "canceled" || state == "error" || state == "lost":
		phase = "complete"
	case eventType == "RUNNER_ONLINE" || eventType == "RESUMED_FROM_CHECKPOINT" || strings.HasPrefix(eventType, "PLAY_") || strings.HasPrefix(eventType, "TASK_") || strings.HasPrefix(eventType, "HOST_"):
		phase = "executing"
	case state == "running" || state == "starting":
		phase = "starting"
	}
	if failureCode != nil {
		return phase, *failureCode
	}
	switch eventType {
	case "HOST_UNREACHABLE":
		return phase, "host_unreachable"
	case "HOST_FAILED":
		return phase, "task_failed"
	case "JOB_FAILED":
		return phase, "execution_failed"
	}
	return phase, ""
}

// SetRelaunchSource links a newly-created job to an authorized source only when
// both jobs are governed by the same unified template.
func (s *JobStore) SetRelaunchSource(ctx context.Context, jobID, sourceJobID, unifiedTemplateID int64) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE unified_jobs target SET source_job_id = source.id
		FROM unified_jobs source
		WHERE target.id=$1 AND source.id=$2
		AND target.unified_job_template_id=$3
		AND source.unified_job_template_id=$3`, jobID, sourceJobID, unifiedTemplateID)
	if err != nil {
		return wrap("JobStore.SetRelaunchSource", err)
	}
	if count, _ := result.RowsAffected(); count != 1 {
		return fmt.Errorf("source job does not belong to the governing template")
	}
	return nil
}

// TemplateIDForRun resolves the job_templates.id governing a run, via
// unified_job -> unified_job_template_id. ok is false when the run has no
// governing template (e.g. an ad-hoc / inventory-sync job) — that is the ONLY
// no-error miss. A real DB error is returned as an error, never masked as
// "no template": masking it would silently degrade the RBAC decision at the
// callsite (a transient outage would read as an unowned run).
func (s *JobStore) TemplateIDForRun(ctx context.Context, runID uuid.UUID) (int64, bool, error) {
	var jtID int64
	err := s.db.GetContext(ctx, &jtID, `
		SELECT jt.id
		FROM execution_runs er
		JOIN unified_jobs uj ON er.unified_job_id = uj.id
		JOIN job_templates jt ON uj.unified_job_template_id = jt.unified_job_template_id
		WHERE er.id = $1`, runID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, wrap("JobStore.TemplateIDForRun", err)
	}
	return jtID, true, nil
}

// InventoryIDForRun resolves the inventory whose host identities appear in a
// run's diagnostics. A nil id is valid for localhost/template-less execution.
func (s *JobStore) InventoryIDForRun(ctx context.Context, runID uuid.UUID) (*int64, error) {
	var inventoryID *int64
	err := s.db.GetContext(ctx, &inventoryID, `
		SELECT jt.inventory_id
		FROM execution_runs er
		JOIN unified_jobs uj ON er.unified_job_id = uj.id
		JOIN job_templates jt ON uj.unified_job_template_id = jt.unified_job_template_id
		WHERE er.id = $1`, runID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, wrap("JobStore.InventoryIDForRun", err)
	}
	return inventoryID, nil
}

// --- write paths (jobs domain) ---

// LaunchTemplateInfo is what LaunchJob needs to validate a launch: the
// job_templates.id that owns the RBAC roles plus the prompt-on-launch config.
type LaunchTemplateInfo struct {
	ID                    int64           `db:"id"`
	AskVariablesOnLaunch  bool            `db:"ask_variables_on_launch"`
	AskLimitOnLaunch      bool            `db:"ask_limit_on_launch"`
	AskInventoryOnLaunch  bool            `db:"ask_inventory_on_launch"`
	AskCredentialOnLaunch bool            `db:"ask_credential_on_launch"`
	SurveyEnabled         bool            `db:"survey_enabled"`
	SurveySpec            json.RawMessage `db:"survey_spec"`
	AllowSimultaneous     bool            `db:"allow_simultaneous"`
}

// LaunchTemplateInfo loads the launch-time template config by unified template id.
func (s *JobStore) LaunchTemplateInfo(ctx context.Context, unifiedTemplateID int64) (LaunchTemplateInfo, error) {
	var jt LaunchTemplateInfo
	err := s.db.GetContext(ctx, &jt,
		`SELECT id, ask_variables_on_launch, ask_limit_on_launch, ask_inventory_on_launch,
		        ask_credential_on_launch, survey_enabled, survey_spec, allow_simultaneous
		 FROM job_templates WHERE unified_job_template_id = $1`, unifiedTemplateID)
	if err != nil {
		return jt, fmt.Errorf("load launch template info: %w", err)
	}
	return jt, nil
}

// ActiveJobCount counts non-terminal jobs for a template — the concurrency guard
// for templates that don't allow simultaneous runs.
func (s *JobStore) ActiveJobCount(ctx context.Context, unifiedTemplateID int64) (int, error) {
	var n int
	err := s.db.GetContext(ctx, &n,
		`SELECT count(*) FROM unified_jobs
		 WHERE unified_job_template_id = $1 AND status NOT IN ('successful','failed','canceled','error')`,
		unifiedTemplateID)
	if err != nil {
		return 0, fmt.Errorf("count active jobs: %w", err)
	}
	return n, nil
}

// InsertPendingJob creates a unified_job in 'pending' state (no current_run_id);
// the scheduler picks it up. Returns the new job id. Delegates to pkg/launch,
// the single job-creation site.
func (s *JobStore) InsertPendingJob(ctx context.Context, name string, unifiedTemplateID int64, opts launch.Options) (int64, error) {
	id, err := launch.Job(ctx, s.db, name, &unifiedTemplateID, opts)
	if err != nil {
		return 0, fmt.Errorf("insert pending job: %w", err)
	}
	return id, nil
}

// UnifiedJobIDForRun returns the unified_job_id owning an execution run.
func (s *JobStore) UnifiedJobIDForRun(ctx context.Context, runID uuid.UUID) (int64, error) {
	var id int64
	err := s.db.QueryRowContext(ctx, `SELECT unified_job_id FROM execution_runs WHERE id = $1`, runID).Scan(&id)
	if err != nil {
		return 0, err // caller distinguishes sql.ErrNoRows (404)
	}
	return id, nil
}

// InsertJobEvent persists a host-runner event; evt.UnifiedJobID/ExecutionRunID
// must be set by the caller. Returns the new event id.
func (s *JobStore) InsertJobEvent(ctx context.Context, evt *models.JobEvent) (int64, error) {
	var id int64
	err := s.db.QueryRowContext(ctx, `
		INSERT INTO job_events (
			unified_job_id, execution_run_id, seq, event_type,
			stdout_snippet, event_data, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id`,
		evt.UnifiedJobID, evt.ExecutionRunID, evt.Seq, evt.EventType,
		evt.StdoutSnippet, evt.EventData, evt.CreatedAt).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("insert job event: %w", err)
	}
	return id, nil
}

// JobCancelInfo is what CancelJob needs: the current status and the governing
// template (nil for a template-less job).
type JobCancelInfo struct {
	Status               string `db:"status"`
	UnifiedJobTemplateID *int64 `db:"unified_job_template_id"`
	InventorySourceID    *int64 `db:"inventory_source_id"`
}

// JobCancelInfo loads a job's status + governing unified template id.
func (s *JobStore) JobCancelInfo(ctx context.Context, jobID int64) (JobCancelInfo, error) {
	var info JobCancelInfo
	err := s.db.GetContext(ctx, &info,
		`SELECT status, unified_job_template_id,
		        CASE WHEN job_args ? 'inventory_source_id' THEN (job_args->>'inventory_source_id')::BIGINT END AS inventory_source_id
		 FROM unified_jobs WHERE id = $1`, jobID)
	if err != nil {
		return info, fmt.Errorf("load job cancel info: %w", err)
	}
	return info, nil
}

// JobTemplateIDByUnified resolves the job_templates.id owning the RBAC roles from
// a unified_job_template_id. ok is false when no such template row exists.
func (s *JobStore) JobTemplateIDByUnified(ctx context.Context, unifiedTemplateID int64) (int64, bool, error) {
	var jtID int64
	err := s.db.GetContext(ctx, &jtID,
		`SELECT id FROM job_templates WHERE unified_job_template_id = $1`, unifiedTemplateID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("resolve template id: %w", err)
	}
	return jtID, true, nil
}

// FlagCancelRequested sets cancel_requested on a running job; its host-runner
// stops the play cooperatively on its next heartbeat.
func (s *JobStore) FlagCancelRequested(ctx context.Context, jobID int64) error {
	if _, err := s.db.ExecContext(ctx,
		`UPDATE unified_jobs SET cancel_requested = true WHERE id = $1`, jobID); err != nil {
		return fmt.Errorf("flag cancel requested: %w", err)
	}
	return nil
}

// CancelNotYetRunning cancels a job that hasn't started executing (pending/queued
// /waiting): flag it canceled and terminate any run row already created, in one
// transaction so a job can't be dispatched between the two updates.
func (s *JobStore) CancelNotYetRunning(ctx context.Context, jobID int64) error {
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin cancel tx: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx,
		`UPDATE unified_jobs SET cancel_requested = true, status = 'canceled', finished_at = now() WHERE id = $1`, jobID); err != nil {
		return fmt.Errorf("cancel unified job: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE execution_runs SET state = 'canceled', finished_at = now() WHERE unified_job_id = $1 AND state NOT IN ('successful','failed','canceled')`, jobID); err != nil {
		return fmt.Errorf("cancel execution runs: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit cancel tx: %w", err)
	}
	return nil
}

// prefixed qualifies a comma-separated column list with a table alias, e.g.
// prefixed("uj", "id, name") -> "uj.id, uj.name".
func prefixed(alias, cols string) string {
	out := ""
	start := 0
	for i := 0; i <= len(cols); i++ {
		if i == len(cols) || cols[i] == ',' {
			col := cols[start:i]
			// trim surrounding spaces
			for len(col) > 0 && col[0] == ' ' {
				col = col[1:]
			}
			if out != "" {
				out += ", "
			}
			out += alias + "." + col
			start = i + 1
		}
	}
	return out
}

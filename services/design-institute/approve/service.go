package approve

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// ══════════════════════════════════════════════════════════════
//  类型定义
//  包装旧系统的 approve_flow + approve_task + approve_flow_record
//  不重造审批引擎，只做业务语义层
// ══════════════════════════════════════════════════════════════

type BizType string

const (
	BizContract   BizType = "CONTRACT"
	BizGathering  BizType = "GATHERING"
	BizBalance    BizType = "BALANCE"
	BizInvoice    BizType = "INVOICE"
	BizCostTicket BizType = "COST_TICKET"
	BizPayment    BizType = "PAYMENT"
)

type FlowState string

const (
	FlowPending   FlowState = "PENDING"
	FlowApproved  FlowState = "APPROVED"
	FlowRejected  FlowState = "REJECTED"
	FlowWithdrawn FlowState = "WITHDRAWN"
)

type TaskState string

const (
	TaskWaiting  TaskState = "WAITING"
	TaskDone     TaskState = "DONE"
	TaskSkipped  TaskState = "SKIPPED"
)

// ApproveFlow 一条审批流
type ApproveFlow struct {
	ID          int64
	LegacyID    *int64
	TenantID    int
	BizType     BizType
	BizID       int64     // 关联的业务对象ID（合同/发票/结算单等）
	BizRef      string    // CoordOS引用（可选）
	Title       string
	Applicant   string
	State       FlowState
	FlowID      *int64    // Activiti流程实例ID
	CreatedAt   time.Time
	UpdatedAt   time.Time
	FinishedAt  *time.Time
}

// ApproveTask 审批节点
type ApproveTask struct {
	ID          int64
	FlowID      int64
	Seq         int
	ApproverRef string   // 审批人（员工ref或角色）
	State       TaskState
	Comment     string
	ActedAt     *time.Time
	CreatedAt   time.Time
}

// ApproveRecord 审批动作日志
type ApproveRecord struct {
	ID         int64
	FlowID     int64
	TaskID     int64
	Action     string   // APPROVE / REJECT / WITHDRAW / TRANSFER
	Actor      string
	Comment    string
	CreatedAt  time.Time
}

type SubmitInput struct {
	BizType    BizType
	BizID      int64
	BizRef     string
	Title      string
	Applicant  string
	Approvers  []string  // 审批人列表（按顺序）
	TenantID   int
}

type ActInput struct {
	FlowID    int64
	TaskID    int64
	Actor     string
	Action    string  // APPROVE / REJECT / WITHDRAW
	Comment   string
}

// ══════════════════════════════════════════════════════════════
//  Store 接口
// ══════════════════════════════════════════════════════════════

type Store interface {
	CreateFlow(ctx context.Context, flow *ApproveFlow) error
	GetFlow(ctx context.Context, id int64) (*ApproveFlow, error)
	UpdateFlowState(ctx context.Context, id int64, state FlowState, finishedAt *time.Time) error
	ListFlowsByBiz(ctx context.Context, bizType BizType, bizID int64) ([]*ApproveFlow, error)
	ListPendingByApprover(ctx context.Context, approverRef string) ([]*ApproveFlow, error)
	ListByTenant(ctx context.Context, tenantID int, state *FlowState, limit, offset int) ([]*ApproveFlow, int, error)

	CreateTask(ctx context.Context, task *ApproveTask) error
	GetTask(ctx context.Context, id int64) (*ApproveTask, error)
	GetCurrentTask(ctx context.Context, flowID int64) (*ApproveTask, error)
	UpdateTaskState(ctx context.Context, id int64, state TaskState, comment string, actedAt time.Time) error
	ListTasksByFlow(ctx context.Context, flowID int64) ([]*ApproveTask, error)

	CreateRecord(ctx context.Context, rec *ApproveRecord) error
	ListRecordsByFlow(ctx context.Context, flowID int64) ([]*ApproveRecord, error)
}

// ══════════════════════════════════════════════════════════════
//  Service
// ══════════════════════════════════════════════════════════════

type Service struct {
	store    Store
	tenantID int
	// onApproved / onRejected 回调：审批完成后通知各业务服务
	onApproved func(ctx context.Context, bizType BizType, bizID int64) error
	onRejected func(ctx context.Context, bizType BizType, bizID int64, reason string) error
}

func NewService(store Store, tenantID int) *Service {
	return &Service{store: store, tenantID: tenantID}
}

func (s *Service) SetCallbacks(
	onApproved func(ctx context.Context, bizType BizType, bizID int64) error,
	onRejected func(ctx context.Context, bizType BizType, bizID int64, reason string) error,
) {
	s.onApproved = onApproved
	s.onRejected = onRejected
}

// ── 提交审批 ──────────────────────────────────────────────────
func (s *Service) Submit(ctx context.Context, in SubmitInput) (*ApproveFlow, error) {
	if len(in.Approvers) == 0 {
		return nil, fmt.Errorf("审批人不能为空")
	}

	flow := &ApproveFlow{
		TenantID:  s.tenantID,
		BizType:   in.BizType,
		BizID:     in.BizID,
		BizRef:    in.BizRef,
		Title:     in.Title,
		Applicant: in.Applicant,
		State:     FlowPending,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	if err := s.store.CreateFlow(ctx, flow); err != nil {
		return nil, fmt.Errorf("创建审批流失败: %w", err)
	}

	// 创建审批节点（串行）
	for i, approver := range in.Approvers {
		task := &ApproveTask{
			FlowID:      flow.ID,
			Seq:         i + 1,
			ApproverRef: approver,
			State:       TaskWaiting,
			CreatedAt:   time.Now(),
		}
		if i > 0 {
			task.State = TaskSkipped // 等前一个完成后激活
		}
		if err := s.store.CreateTask(ctx, task); err != nil {
			return nil, fmt.Errorf("创建审批任务失败: %w", err)
		}
	}

	return flow, nil
}

// ── 审批动作（通过/拒绝/撤回） ────────────────────────────────
func (s *Service) Act(ctx context.Context, in ActInput) error {
	flow, err := s.store.GetFlow(ctx, in.FlowID)
	if err != nil {
		return fmt.Errorf("审批流不存在: %w", err)
	}
	if flow.State != FlowPending {
		return fmt.Errorf("审批流已结束，状态: %s", flow.State)
	}

	task, err := s.store.GetCurrentTask(ctx, in.FlowID)
	if err != nil {
		return fmt.Errorf("获取当前审批节点失败: %w", err)
	}
	if task.ApproverRef != in.Actor {
		return fmt.Errorf("当前审批人为 %s，%s 无权审批", task.ApproverRef, in.Actor)
	}

	now := time.Now()

	// 记录审批日志
	s.store.CreateRecord(ctx, &ApproveRecord{
		FlowID:    in.FlowID,
		TaskID:    task.ID,
		Action:    in.Action,
		Actor:     in.Actor,
		Comment:   in.Comment,
		CreatedAt: now,
	})

	switch in.Action {
	case "APPROVE":
		s.store.UpdateTaskState(ctx, task.ID, TaskDone, in.Comment, now)
		// 查下一个节点
		tasks, _ := s.store.ListTasksByFlow(ctx, in.FlowID)
		nextTask := s.findNextTask(tasks, task.Seq)
		if nextTask != nil {
			// 激活下一个节点
			s.store.UpdateTaskState(ctx, nextTask.ID, TaskWaiting, "", now)
		} else {
			// 全部通过，流程结束
			s.store.UpdateFlowState(ctx, in.FlowID, FlowApproved, &now)
			if s.onApproved != nil {
				s.onApproved(ctx, flow.BizType, flow.BizID)
			}
		}

	case "REJECT":
		s.store.UpdateTaskState(ctx, task.ID, TaskDone, in.Comment, now)
		s.store.UpdateFlowState(ctx, in.FlowID, FlowRejected, &now)
		if s.onRejected != nil {
			s.onRejected(ctx, flow.BizType, flow.BizID, in.Comment)
		}

	case "WITHDRAW":
		if flow.Applicant != in.Actor {
			return fmt.Errorf("只有申请人 %s 可以撤回", flow.Applicant)
		}
		s.store.UpdateFlowState(ctx, in.FlowID, FlowWithdrawn, &now)

	default:
		return fmt.Errorf("未知操作: %s", in.Action)
	}

	return nil
}

// ── 查询 ──────────────────────────────────────────────────────
func (s *Service) GetFlow(ctx context.Context, id int64) (*ApproveFlow, error) {
	return s.store.GetFlow(ctx, id)
}

func (s *Service) GetFlowDetail(ctx context.Context, id int64) (*ApproveFlow, []*ApproveTask, []*ApproveRecord, error) {
	flow, err := s.store.GetFlow(ctx, id)
	if err != nil {
		return nil, nil, nil, err
	}
	tasks, err := s.store.ListTasksByFlow(ctx, id)
	if err != nil {
		return nil, nil, nil, err
	}
	records, err := s.store.ListRecordsByFlow(ctx, id)
	if err != nil {
		return nil, nil, nil, err
	}
	return flow, tasks, records, nil
}

func (s *Service) ListPending(ctx context.Context, approverRef string) ([]*ApproveFlow, error) {
	return s.store.ListPendingByApprover(ctx, approverRef)
}

func (s *Service) ListByBiz(ctx context.Context, bizType BizType, bizID int64) ([]*ApproveFlow, error) {
	return s.store.ListFlowsByBiz(ctx, bizType, bizID)
}

func (s *Service) findNextTask(tasks []*ApproveTask, currentSeq int) *ApproveTask {
	for _, t := range tasks {
		if t.Seq == currentSeq+1 {
			return t
		}
	}
	return nil
}

// ══════════════════════════════════════════════════════════════
//  PostgreSQL Store 实现
// ══════════════════════════════════════════════════════════════

type PGStore struct{ db *sql.DB }

func NewPGStore(db *sql.DB) Store { return &PGStore{db: db} }

func (s *PGStore) CreateFlow(ctx context.Context, flow *ApproveFlow) error {
	return s.db.QueryRowContext(ctx, `
		INSERT INTO approve_flows (
			tenant_id, biz_type, biz_id, biz_ref, title,
			applicant, state, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id`,
		flow.TenantID, flow.BizType, flow.BizID, flow.BizRef, flow.Title,
		flow.Applicant, flow.State, flow.CreatedAt, flow.UpdatedAt,
	).Scan(&flow.ID)
}

func (s *PGStore) GetFlow(ctx context.Context, id int64) (*ApproveFlow, error) {
	f := &ApproveFlow{}
	err := s.db.QueryRowContext(ctx, `
		SELECT id,legacy_id,tenant_id,biz_type,biz_id,biz_ref,
		       title,applicant,state,flow_id,
		       created_at,updated_at,finished_at
		FROM approve_flows WHERE id=$1`, id,
	).Scan(&f.ID, &f.LegacyID, &f.TenantID, &f.BizType, &f.BizID, &f.BizRef,
		&f.Title, &f.Applicant, &f.State, &f.FlowID,
		&f.CreatedAt, &f.UpdatedAt, &f.FinishedAt)
	return f, err
}

func (s *PGStore) UpdateFlowState(ctx context.Context, id int64, state FlowState, finishedAt *time.Time) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE approve_flows SET state=$1, finished_at=$2, updated_at=NOW() WHERE id=$3`,
		state, finishedAt, id)
	return err
}

func (s *PGStore) ListFlowsByBiz(ctx context.Context, bizType BizType, bizID int64) ([]*ApproveFlow, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id,legacy_id,tenant_id,biz_type,biz_id,biz_ref,
		       title,applicant,state,flow_id,created_at,updated_at,finished_at
		FROM approve_flows WHERE biz_type=$1 AND biz_id=$2 ORDER BY created_at DESC`,
		bizType, bizID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanFlows(rows)
}

func (s *PGStore) ListPendingByApprover(ctx context.Context, approverRef string) ([]*ApproveFlow, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT f.id,f.legacy_id,f.tenant_id,f.biz_type,f.biz_id,f.biz_ref,
		       f.title,f.applicant,f.state,f.flow_id,f.created_at,f.updated_at,f.finished_at
		FROM approve_flows f
		JOIN approve_tasks t ON t.flow_id=f.id
		WHERE f.state='PENDING' AND t.approver_ref=$1 AND t.state='WAITING'
		ORDER BY f.created_at`, approverRef)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanFlows(rows)
}

func (s *PGStore) ListByTenant(ctx context.Context, tenantID int,
	state *FlowState, limit, offset int) ([]*ApproveFlow, int, error) {
	where, args := "tenant_id=$1", []any{tenantID}
	if state != nil {
		where += " AND state=$2"
		args = append(args, *state)
	}
	var total int
	s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM approve_flows WHERE "+where, args...).Scan(&total)
	args = append(args, limit, offset)
	n := len(args)
	rows, err := s.db.QueryContext(ctx,
		`SELECT id,legacy_id,tenant_id,biz_type,biz_id,biz_ref,
		        title,applicant,state,flow_id,created_at,updated_at,finished_at
		 FROM approve_flows WHERE `+where+
			fmt.Sprintf(` ORDER BY created_at DESC LIMIT $%d OFFSET $%d`, n-1, n), args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	flows, err := scanFlows(rows)
	return flows, total, err
}

func (s *PGStore) CreateTask(ctx context.Context, task *ApproveTask) error {
	return s.db.QueryRowContext(ctx, `
		INSERT INTO approve_tasks (flow_id, seq, approver_ref, state, created_at)
		VALUES ($1,$2,$3,$4,$5) RETURNING id`,
		task.FlowID, task.Seq, task.ApproverRef, task.State, task.CreatedAt,
	).Scan(&task.ID)
}

func (s *PGStore) GetTask(ctx context.Context, id int64) (*ApproveTask, error) {
	t := &ApproveTask{}
	err := s.db.QueryRowContext(ctx,
		`SELECT id,flow_id,seq,approver_ref,state,comment,acted_at,created_at
		 FROM approve_tasks WHERE id=$1`, id,
	).Scan(&t.ID, &t.FlowID, &t.Seq, &t.ApproverRef,
		&t.State, &t.Comment, &t.ActedAt, &t.CreatedAt)
	return t, err
}

func (s *PGStore) GetCurrentTask(ctx context.Context, flowID int64) (*ApproveTask, error) {
	t := &ApproveTask{}
	err := s.db.QueryRowContext(ctx, `
		SELECT id,flow_id,seq,approver_ref,state,comment,acted_at,created_at
		FROM approve_tasks WHERE flow_id=$1 AND state='WAITING'
		ORDER BY seq LIMIT 1`, flowID,
	).Scan(&t.ID, &t.FlowID, &t.Seq, &t.ApproverRef,
		&t.State, &t.Comment, &t.ActedAt, &t.CreatedAt)
	return t, err
}

func (s *PGStore) UpdateTaskState(ctx context.Context, id int64, state TaskState, comment string, actedAt time.Time) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE approve_tasks SET state=$1, comment=$2, acted_at=$3 WHERE id=$4`,
		state, comment, actedAt, id)
	return err
}

func (s *PGStore) ListTasksByFlow(ctx context.Context, flowID int64) ([]*ApproveTask, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id,flow_id,seq,approver_ref,state,comment,acted_at,created_at
		 FROM approve_tasks WHERE flow_id=$1 ORDER BY seq`, flowID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tasks []*ApproveTask
	for rows.Next() {
		t := &ApproveTask{}
		if err := rows.Scan(&t.ID, &t.FlowID, &t.Seq, &t.ApproverRef,
			&t.State, &t.Comment, &t.ActedAt, &t.CreatedAt); err != nil {
			return nil, err
		}
		tasks = append(tasks, t)
	}
	return tasks, rows.Err()
}

func (s *PGStore) CreateRecord(ctx context.Context, rec *ApproveRecord) error {
	return s.db.QueryRowContext(ctx, `
		INSERT INTO approve_records (flow_id, task_id, action, actor, comment, created_at)
		VALUES ($1,$2,$3,$4,$5,$6) RETURNING id`,
		rec.FlowID, rec.TaskID, rec.Action, rec.Actor, rec.Comment, rec.CreatedAt,
	).Scan(&rec.ID)
}

func (s *PGStore) ListRecordsByFlow(ctx context.Context, flowID int64) ([]*ApproveRecord, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id,flow_id,task_id,action,actor,comment,created_at
		 FROM approve_records WHERE flow_id=$1 ORDER BY created_at`, flowID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var recs []*ApproveRecord
	for rows.Next() {
		r := &ApproveRecord{}
		if err := rows.Scan(&r.ID, &r.FlowID, &r.TaskID,
			&r.Action, &r.Actor, &r.Comment, &r.CreatedAt); err != nil {
			return nil, err
		}
		recs = append(recs, r)
	}
	return recs, rows.Err()
}

func scanFlows(rows *sql.Rows) ([]*ApproveFlow, error) {
	var list []*ApproveFlow
	for rows.Next() {
		f := &ApproveFlow{}
		if err := rows.Scan(&f.ID, &f.LegacyID, &f.TenantID, &f.BizType, &f.BizID, &f.BizRef,
			&f.Title, &f.Applicant, &f.State, &f.FlowID,
			&f.CreatedAt, &f.UpdatedAt, &f.FinishedAt); err != nil {
			return nil, err
		}
		list = append(list, f)
	}
	return list, rows.Err()
}

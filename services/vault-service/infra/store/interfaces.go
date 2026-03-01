// Package store defines persistence interfaces for vault-service.
//
// Application/domain code only depends on these interfaces. Concrete
// implementations (rocksdb) are injected at startup.
package store

import (
	"time"

	pc "coordos/project-core"
)

type ProjectTreeStore interface {
	CreateNode(tenantID string, node *pc.ProjectNode) error
	GetNode(tenantID string, ref pc.VRef) (*pc.ProjectNode, error)
	GetChildren(tenantID string, ref pc.VRef) ([]*pc.ProjectNode, error)
	GetAncestors(tenantID string, ref pc.VRef) ([]*pc.ProjectNode, error)
	UpdateStatus(tenantID string, ref pc.VRef, status pc.LifecycleStatus) error
	UpdateNode(tenantID string, node *pc.ProjectNode) error
	ValidateChildConstraint(tenantID string, child *pc.ProjectNode) error
	ListByTenant(tenantID string, filter ProjectFilter) ([]*pc.ProjectNode, int64, error)
}

type ProjectFilter struct {
	Status   *pc.LifecycleStatus
	OwnerRef *pc.VRef
	Limit    int
	Offset   int
}

type GenesisStore interface {
	CreateFull(tenantID string, g *pc.GenesisUTXOFull) error
	GetFull(tenantID string, ref pc.VRef) (*pc.GenesisUTXOFull, error)
	UpdateFull(tenantID string, g *pc.GenesisUTXOFull) error
	GetRemainingQuota(tenantID string, ref pc.VRef) (int64, error)
	ListByProject(tenantID string, projectRef pc.VRef) ([]*pc.GenesisUTXOFull, error)
}

type ContractStore interface {
	Create(tenantID string, c *Contract) error
	Get(tenantID string, ref pc.VRef) (*Contract, error)
	Update(tenantID string, c *Contract) error
	List(tenantID string, f ContractFilter) ([]*Contract, int64, error)
	GetRemainingAmount(tenantID string, ref pc.VRef) (int64, error)
	ValidatePayment(tenantID string, ref pc.VRef, amount int64) error
}

type Contract struct {
	Ref              pc.VRef          `json:"ref"`
	ProjectRef       pc.VRef          `json:"project_ref"`
	ProcurementRef   pc.VRef          `json:"procurement_ref"`
	ContractKind     string           `json:"contract_kind"`
	ContractNo       string           `json:"contract_no"`
	ContractName     string           `json:"contract_name"`
	PartyA           pc.VRef          `json:"party_a"`
	PartyB           pc.VRef          `json:"party_b"`
	BranchRef        pc.VRef          `json:"branch_ref"`
	ManagerRef       pc.VRef          `json:"manager_ref"`
	AmountWithTax    int64            `json:"amount_with_tax"`
	AmountWithoutTax int64            `json:"amount_without_tax"`
	TaxRate          float64          `json:"tax_rate"`
	SignDate         string           `json:"sign_date"`
	EffectiveDate    string           `json:"effective_date"`
	ExpiryDate       string           `json:"expiry_date"`
	PaymentNodes     []pc.PaymentNode `json:"payment_nodes"`
	SealStatus       string           `json:"seal_status"`
	AttachmentRefs   []pc.VRef        `json:"attachment_refs"`
	Status           string           `json:"status"`
	TenantID         string           `json:"tenant_id"`
	CreatedAt        time.Time        `json:"created_at"`
	ProofHash        string           `json:"proof_hash"`
}

type ContractFilter struct {
	ProjectRef *pc.VRef
	BranchRef  *pc.VRef
	Status     *string
	Kind       *string
	Limit      int
	Offset     int
}

type ParcelStore interface {
	Create(tenantID string, p *Parcel) error
	Get(tenantID string, ref pc.VRef) (*Parcel, error)
	Update(tenantID string, p *Parcel) error
	ListByProject(tenantID string, projectRef pc.VRef) ([]*Parcel, error)
	ListByContract(tenantID string, contractRef pc.VRef) ([]*Parcel, error)
}

type Parcel struct {
	Ref         pc.VRef                `json:"ref"`
	ProjectRef  pc.VRef                `json:"project_ref"`
	ContractRef pc.VRef                `json:"contract_ref"`
	Class       string                 `json:"class"`
	Name        string                 `json:"name"`
	Status      string                 `json:"status"`
	TenantID    string                 `json:"tenant_id"`
	CreatedAt   time.Time              `json:"created_at"`
	ProofHash   string                 `json:"proof_hash"`
	Payload     map[string]interface{} `json:"payload"`
}

type UTXOStore interface {
	Create(tenantID string, u *UTXO) error
	Get(tenantID string, ref pc.VRef) (*UTXO, error)
	Update(tenantID string, u *UTXO) error
	ListByProject(tenantID string, projectRef pc.VRef) ([]*UTXO, error)
	ListByParcel(tenantID string, parcelRef pc.VRef) ([]*UTXO, error)
}

type UTXO struct {
	Ref        pc.VRef                `json:"ref"`
	ProjectRef pc.VRef                `json:"project_ref"`
	ParcelRef  pc.VRef                `json:"parcel_ref"`
	GenesisRef pc.VRef                `json:"genesis_ref"`
	Kind       string                 `json:"kind"`
	Status     string                 `json:"status"`
	TenantID   string                 `json:"tenant_id"`
	CreatedAt  time.Time              `json:"created_at"`
	ProofHash  string                 `json:"proof_hash"`
	PrevHash   string                 `json:"prev_hash"`
	Payload    map[string]interface{} `json:"payload"`
}

type SettlementStore interface {
	Create(tenantID string, s *Settlement) error
	Get(tenantID string, ref pc.VRef) (*Settlement, error)
	Update(tenantID string, s *Settlement) error
	ListByProject(tenantID string, projectRef pc.VRef) ([]*Settlement, error)
}

type Settlement struct {
	Ref        pc.VRef   `json:"ref"`
	ProjectRef pc.VRef   `json:"project_ref"`
	GenesisRef pc.VRef   `json:"genesis_ref"`
	Amount     int64     `json:"amount"`
	Status     string    `json:"status"`
	TenantID   string    `json:"tenant_id"`
	CreatedAt  time.Time `json:"created_at"`
	ProofHash  string    `json:"proof_hash"`
}

type WalletStore interface {
	GetOrCreate(tenantID string, ownerRef pc.VRef) (*Wallet, error)
	Credit(tenantID string, ownerRef pc.VRef, amount int64, note string) error
	Debit(tenantID string, ownerRef pc.VRef, amount int64, note string) error
	GetBalance(tenantID string, ownerRef pc.VRef) (int64, error)
	ListLedger(tenantID string, ownerRef pc.VRef, limit int) ([]*LedgerEntry, error)
}

type Wallet struct {
	OwnerRef  pc.VRef   `json:"owner_ref"`
	Balance   int64     `json:"balance"`
	TenantID  string    `json:"tenant_id"`
	UpdatedAt time.Time `json:"updated_at"`
}

type LedgerEntry struct {
	ID        string    `json:"id"`
	OwnerRef  pc.VRef   `json:"owner_ref"`
	Amount    int64     `json:"amount"` // positive=credit, negative=debit
	Note      string    `json:"note"`
	CreatedAt time.Time `json:"created_at"`
}

type AuditStore interface {
	RecordEvent(tenantID string, evt AuditEvent) (string, error)
	RecordViolation(tenantID, rule string, evt AuditEvent, detail string) (string, error)
	QueryEvents(tenantID string, f AuditFilter) ([]AuditEvent, error)
}

type AuditEvent struct {
	EventID    string                 `json:"event_id"`
	TenantID   string                 `json:"tenant_id"`
	ProjectRef pc.VRef                `json:"project_ref"`
	ActorRef   pc.VRef                `json:"actor_ref"`
	Verb       string                 `json:"verb"`
	Payload    map[string]interface{} `json:"payload"`
	ProofHash  string                 `json:"proof_hash"`
	Timestamp  time.Time              `json:"timestamp"`
}

type AuditFilter struct {
	ProjectRef *pc.VRef
	ActorRef   *pc.VRef
	Verb       *string
	From       *time.Time
	To         *time.Time
	Limit      int
}

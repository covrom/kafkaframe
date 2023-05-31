package hr

import (
	"context"
	"strings"
	"time"

	"github.com/covrom/kafkaframe"
	"github.com/covrom/kafkaframe/examples/producer/etl"
	"github.com/covrom/kafkaframe/examples/producer/kafkaemployees"
	"github.com/rs/xid"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

type UsersTask struct {
	kfk *kafkaframe.Kafka
}

func NewUsersTask(kfk *kafkaframe.Kafka) *UsersTask {
	ret := &UsersTask{
		kfk: kfk,
	}
	return ret
}

func (s *UsersTask) String() string {
	return "users"
}

func (s *UsersTask) ExecuteSync(ctx context.Context) error {
	exctx := etl.ExecuteContext{
		Kfk:    s.kfk,
		Loader: s,
	}
	return exctx.ExecuteSync(ctx)
}

type SourceUser struct {
	Id        string     `json:"id"`
	CreatedAt time.Time  `json:"createdAt"`
	UpdatedAt time.Time  `json:"updatedAt"`
	DeletedAt *time.Time `json:"deletedAt,omitempty"`

	Login          string `json:"userName"`
	FullName       string `json:"fullName"`
	Email          string `json:"email"`
	Phone          string `json:"phone"`
	SupervisorID   string `json:"supervisorID"`
	OrganizationID string `json:"organizationID"`
	PositionTitle  string `json:"positionTitle"`
	UnitID         string `json:"unitID"`
	LocationID     string `json:"locationID"`
}

func (e SourceUser) GetId() string {
	return e.Id
}

type HRUsersModify kafkaemployees.HRUsersModify

func (x *HRUsersModify) HREvent() kafkaemployees.ProtoEvent {
	return (*kafkaemployees.HRUsersModify)(x)
}

func (x *HRUsersModify) Len() int {
	return len(x.Users)
}

func (x *HRUsersModify) TopicName() string {
	return kafkaemployees.UsersModifyTopicName
}

func (s *UsersTask) FullSync(ctx context.Context, taskId xid.ID, chbun chan etl.SyncObjer) time.Time {
	fctx := etl.FullSyncContext[SourceUser, *kafkaemployees.HRUser]{
		Fetcher:   s.fetchUserAll,
		Convertor: s.ConvertUser(ctx),
		Builder: func(taskId xid.ID, els []*kafkaemployees.HRUser) etl.SyncObjer {
			return &HRUsersModify{
				Event: &kafkaemployees.Event{
					Id:        xid.New().String(),
					CreatedAt: timestamppb.Now(),
					TaskID:    taskId.String(),
				},
				Users: els,
			}
		},
	}
	return fctx.FullSync(ctx, taskId, chbun)
}

func (s *UsersTask) fetchUserAll(ctx context.Context, chout chan []SourceUser) {
	defer close(chout)

	// here you must fetch users from source
	chout <- []SourceUser{}
}

func (s *UsersTask) ConvertUser(ctx context.Context) func(SourceUser) *kafkaemployees.HRUser {
	return func(respusr SourceUser) *kafkaemployees.HRUser {
		usr := &kafkaemployees.HRUser{
			Id:             respusr.Id,
			CreatedAt:      timestamppb.New(respusr.CreatedAt),
			UpdatedAt:      timestamppb.New(respusr.UpdatedAt),
			UserName:       strings.ToLower(respusr.Login),
			Email:          respusr.Email,
			Phone:          respusr.Phone,
			OrganizationID: respusr.OrganizationID,
			SupervisorID:   respusr.SupervisorID,
			LocationID:     respusr.LocationID,
			UnitID:         respusr.UnitID,
		}

		if respusr.DeletedAt != nil && !respusr.DeletedAt.IsZero() {
			usr.DeletedAt = timestamppb.New(*respusr.DeletedAt)
		}

		usr.UpdateHash64()
		return usr
	}
}

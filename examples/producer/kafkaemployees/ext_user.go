package kafkaemployees

import (
	"fmt"
	"hash/fnv"
)

func (u *HRUser) CalcHash64() uint64 {
	h := fnv.New64a()
	fmt.Fprintln(h,
		u.Id,
		u.UserName,
		u.FullName,
		u.Email,
		u.Phone,
		u.SupervisorID,
		u.OrganizationID,
		u.PositionTitle,
		u.UnitID,
		u.LocationID,
	)

	return h.Sum64()
}

func (u *HRUser) UpdateHash64() {
	u.HashSum64 = u.CalcHash64()
}

func (u *HRUser) IsZero() bool {
	return u == nil || u.Id == "" || u.CreatedAt.AsTime().IsZero()
}

func (t *HRUsersModify) Len() int {
	return len(t.Users)
}

func (t *HRUsersDelete) Len() int {
	return len(t.UserIds)
}
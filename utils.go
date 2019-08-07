// Copyright (c) 2019 AccelByte Inc. All Rights Reserved.
// This is licensed software from AccelByte Inc, for limitations
// and restrictions contact your company contract manager.

package messagebus

import (
	"strings"

	"github.com/google/uuid"
)

// generateID returns UUID without dash
func generateID() string {
	id := uuid.New()
	return strings.Replace(id.String(), "-", "", -1)
}

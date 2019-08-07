# Copyright 2019 AccelByte Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

 clean:
	rm coverage.out

test:
	docker-compose -f docker-compose-test.yml up -d
	sleep 30
	CGO_ENABLED=0 go test -v -cover ./...
	docker-compose -f docker-compose-test.yml stop

coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out
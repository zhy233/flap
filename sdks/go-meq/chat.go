//  Copyright © 2018 Sunface <CTO@188.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package meq

import (
	"flap_zhy/proto"
	"flap_zhy/proto/mqtt"
)

func (c *Connection) JoinChat(topic []byte) {
	m := mqtt.Publish{
		Header: &mqtt.StaticHeader{
			QOS: 0,
		},
		Topic:   topic,
		Payload: proto.PackJoinChat(topic),
	}
	m.EncodeTo(c.conn)
}

func (c *Connection) LeaveChat(topic []byte) {
	m := mqtt.Publish{
		Header: &mqtt.StaticHeader{
			QOS: 0,
		},
		Topic:   topic,
		Payload: proto.PackLeaveChat(topic),
	}
	m.EncodeTo(c.conn)
}

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

import "flap_zhy/proto"

type PubMsgHandler func(*proto.PubMsg)
type UnreadHandler func([]byte, int)

func (c *Connection) OnMessage(h PubMsgHandler) {
	c.pubmsgh = h
}

func (c *Connection) OnUnread(h UnreadHandler) {
	c.unreadh = h
}

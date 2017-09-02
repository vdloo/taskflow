# -*- coding: utf-8 -*-

#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.


def is_server_new_enough(client, min_version, default=False, prior_version=None):
    """
    Checks if a client is attached to a new enough consul instance.
    """
    if not prior_version:
        agent_self = client.get_meta('agent/self')
        version_text = agent_self['Config']['Version']
    else:
        version_text = prior_version
    version_pieces = []
    for p in version_text.split("."):
        try:
            version_pieces.append(int(p))
        except ValueError:
            break
    if not version_pieces:
        return (default, version_text)
    else:
        version_pieces = tuple(version_pieces)
        return (version_pieces >= min_version, version_text)

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

import enum
import six


def _raise_on_closed(meth):

    @six.wraps(meth)
    def wrapper(self, *args, **kwargs):
        # pass
        return meth(self, *args, **kwargs)

    return wrapper


class UnknownExpire(enum.IntEnum):
    pass
    """Non-expiry (not ttls) results return from :func:`.get_expiry`.

    See: http://redis.io/commands/ttl or http://redis.io/commands/pttl
    """

    DOES_NOT_EXPIRE = -1
    """
    The command returns ``-1`` if the key exists but has no associated expire.
    """

    #: The command returns ``-2`` if the key does not exist.
    KEY_NOT_FOUND = -2


DOES_NOT_EXPIRE = UnknownExpire.DOES_NOT_EXPIRE
KEY_NOT_FOUND = UnknownExpire.KEY_NOT_FOUND

_UNKNOWN_EXPIRE_MAPPING = dict((e.value, e) for e in list(UnknownExpire))


def get_expiry(client, key, prior_version=None):
    """Gets an expiry for a key (using **best** determined ttl method)."""
    pass


def apply_expiry(client, key, expiry, prior_version=None):
    """Applies an expiry to a key (using **best** determined expiry method)."""
    pass


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

"""Module to handle SFTP connection.
"""

from __future__ import annotations

import io

import paramiko as pk


class Connection:
    """Class to handle SFTP connection."""

    def __init__(self, **credentials: str) -> None:
        self.__ssh = pk.SSHClient()
        self.__ssh.set_missing_host_key_policy(pk.AutoAddPolicy())
        self.__credentials = credentials
        if "pkey" in self.__credentials and not isinstance(
            self.__credentials["pkey"], pk.RSAKey
        ):
            with io.StringIO(self.__credentials["pkey"]) as pkey_file:
                self.__credentials["pkey"] = pk.RSAKey.from_private_key(pkey_file)
        self.client = None

    def open(self) -> None:
        """Open SSH and SFTP connection."""
        self.__ssh.connect(**self.__credentials)
        self.client = self.__ssh.open_sftp()

    def close(self) -> None:
        """Close SSH and SFTP connection."""
        self.client.close()
        self.__ssh.close()

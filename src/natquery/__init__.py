"""NatQuery - ask your database questions in plain English, with role-based access control.

from natquery import NatQuery
nq = NatQuery.from_workspace()
session = nq.login({"username": "...", "password": "..."})
response = nq.ask(session, "how many students are in CSE?")
"""

__version__ = "2.0.0"


def __getattr__(name):  # lazy: importing `natquery` must stay cheap
    if name == "NatQuery":
        from natquery.app import NatQueryApp

        return NatQueryApp
    raise AttributeError(name)

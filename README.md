spade
==================

This repository contains the software powering the ♠️ (Spade) API:
A Fil-Network Storage-Proposal Management Service.

### SP Preparation

* Setup authentication you will be continuously using to validate that you are
the current operator of a particular SP system. Authentication is based on having
access to your SP **Worker Key**, the same one you use to submit ProveCommits
when onboarding new sectors. You can either use the reference authenticator script,
or write your own [based on the simple steps described within](https://github.com/ribasushi/bash-fil-spid-v0/blob/5f41eec1a/fil-spid.bash#L13-L33).
It is also likely this functionality will be soon included in Fil implementations directly.

  A. Download the authenticator: `curl -OL https://raw.githubusercontent.com/ribasushi/bash-fil-spid-v0/5f41eec1a/fil-spid.bash`

  B. Make it executable `chmod 755 fil-spid.bash`

  C. Use it as part of your requests, e.g: `curl -sLH "Authorization: $( ./fil-spid.bash f0XXXX )" https://api.spade.storage/sp/status`

### SP Dealmaking

Workflow has **changed substantially**. This entry will be updated when the final API solidifies in the coming few days.
In the meantime join us in [#spade over at the Fil Slack], while we clean up the dust 🤩

[API]: https://raw.githubusercontent.com/ribasushi/spade/master/webapi/routes.go
[#spade over at the Fil Slack]: https://filecoinproject.slack.com/archives/C0377FJCG1L

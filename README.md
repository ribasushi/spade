filecoin-evergreen-dealer
==================

This repository contains the software powering the EverGreen Dealer (EGD):
A Filecoin Storage-Proposal Management Service.

EGD provides a low-friction environment for Storage Providers (SP) to receive a
virtually unlimited volume of FilecoinPlus (Fil+) denominated storage deals.
It does this by aggregating storage requests and metadata from various sources
(EGD tenants) and then presenting the aggregate as a single machine- and
human-readable stream to participating SPs. SPs in turn consume this aggregated
list and can trigger an instant storage deal proposal for any entry, as long as
the proposal does not violate the terms set by the originating tenant.

A distinct feature of an EGD service is its focus on throughput 🚀 and thus its
design exclusively around a "pull" workflow. A deal proposal can only be initiated
by the receiving SP and all deal proposals are, without exception, made for
**out-of-band data flow** (often mislabeled *offline deals*). This gives **complete
control to the SP operator** over both the deal-transfer mechanism, and the
timing of data injection into their carefully tuned sealing pipeline 😻

From a tenant perspective the service provides a convenient way to disseminate
a dataset to a group of storage providers while strictly following replication
guidelines set by the tenant themselves. In order to become a tenant, all a data
supplier needs to define is a replication policy, a desired list of `PieceCID`s
and their description, and then to make the corresponding `CAR` files available
over HTTP or comparable stream-oriented protocol. The service then takes care of
everything else, even selecting Storage Providers automatically if desired by
the tenant.

**NOTE**: At the time of writing the onboarding experience for those just joining
the service is nowhere near as smooth as it could and should be 😞 The final
streamlined version of self-signups for both tenants and SPs as we envision it
is still being worked on 👷 and we sincerely apologize for all the dust. Tentative
ETA for completion of all work is the end of 2022. Please visit us in
[#slingshot-evergreen over at the Filecoin Slack] with any further questions.

## Workflow

From the point of a Storage Provider wanting to receive deals from the service,
the steps are as follows:

### SP Preparation

* Setup authentication you will be continuously using to validate that you are
the current operator of a particular SP system. Authentication is based on having
access to your SP **Worker Key**, the same one you use to submit ProveCommits
when onboarding new sectors. You can either use the reference authenticator script,
or write your own [based on the simple steps described within](https://github.com/filecoin-project/evergreen-dealer/blob/6ebf5230c7ec/misc/fil-spid.bash#L19-L30).
It is also likely this functionality will be soon included in Filecoin implementations directly.

  A. Download the authenticator: `curl -OL https://raw.githubusercontent.com/filecoin-project/evergreen-dealer/6ebf5230c7ec/misc/fil-spid.bash`

  B. Make it executable `chmod 755 fil-spid.bash`

  C. Use it as part of your requests, e.g: `curl -sLH "Authorization: $( ./fil-spid.bash f0XXXX )" https://api.evergreen.filecoin.io/sp/pending_proposals`


* Register with one or more of the currently available tenants. Any authenticated SP can examine the current list of tenants, the nature of their content, and their various sign-up and service conditions, by simply calling
https://api.evergreen.filecoin.io/sp/status.

### SP Dealmaking

* After the above steps are completed an SP is ready to participate in an all-you-can-seal 🦭 buffet, by iterating over the following [API] calls.
Note that you are not confined to using `curl`, which is simply used for simplicity of the examples. You can and are encouraged to develop your own small program autonomously consuming the EGD [API].

  1. Use `/sp/eligible_pieces` to examine the lists of `PieceCID`s and potential data-sources which your SP is eligible to seal. Note that you can skip this step if you are working with a tenant directly: just move to 2) if you already have a list of `PieceCID`s.

     `curl -sLH "Authorization: $( ./fil-spid.bash f0xxxx )" https://api.evergreen.filecoin.io/sp/eligible_pieces | less`


  2. Use `/sp/request_piece/baga...` to request a deal proposal for every `PieceCID` whose data you can reasonably obtain. It is perfectly ok to request a deal, as a type of reservation, even if you are unsure whether you will be able to get the corresponding data. You will be given a tenant-controlled amount of days, and a proposal-bytes-in-flight allotment providing sufficient amount of time to obtain the necessary deal data. If you fail to do so: the proposal simply expires without consequences once it reaches its DealStartEpoch.

     `curl -sLH "Authorization: $( ./fil-spid.bash f0xxxx )" https://api.evergreen.filecoin.io/sp/request_piece/bagaChosenPieceCidxxxxxxxxxxxxxxx`

     About ~5 minutes after invoking this method your SP system should receive a deal proposal for the requested `PieceCID`. Its deal-start-time and other parameters are determined by the corresponding tenant providing the `PieceCID`.

     Note that in order to prevent over-reservation of Fil+, each tenant sets an upper limit of how many outstanding proposals there can be against a specific SP.

  3. Inject the data into your SP when you are ready to seal 🦭 ‼️ You can use `/sp/pending_proposals` at any time to view the outstanding deals against your SP.

     `curl -sLH "Authorization: $( ./fil-spid.bash f0xxxx )" https://api.evergreen.filecoin.io/sp/pending_proposals`


  * Repeat steps 1, 2 and 3 over and over again for each individual `PieceCID`. You are strongly encouraged to automate this process: a typical SP would go through hundreds 💯 such interactions every day.

In case of any difficulties or issues, don't hesitate to contact us in [#slingshot-evergreen over at the Filecoin Slack]: we are happy to hear from you 🤩

[API]: https://raw.githubusercontent.com/filecoin-project/evergreen-dealer/master/webapi/routes.go
[#slingshot-evergreen over at the Filecoin Slack]: https://filecoinproject.slack.com/archives/C0377FJCG1L


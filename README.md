filecoin-evergreen-dealer
==================

This is the engine powering the [`Filecoin Evergreen Program`](https://evergreen.filecoin.io/program-details). Once you have familiarized yourself with the program details come back and follow the simple checklist!

## Terms and Eligibility

- Participation implies your agreement to continuously serve accepted Fil+ deals for a period of about **~532 days**, in an open manner, at storage and retrieval price of 0 ( meaning **free storage and retrieval** ). There are no exceptions to this requirement, and it will be enforced systematically, with associated permanent records in the appropriate reputations systems.
- All deal proposals will be FilecoinPlus verified, with expected activation within **72 hours** from the time of request. They might come from any of the addresses listed in the [public client address list](https://api.evergreen.filecoin.io/public/clients.txt). Note that this list might get updated in the future, so refresh it periodically.
- In order to participate, you [need to register](https://docs.google.com/forms/d/e/1FAIpQLSe5bpkD5RJeHGMNx3CpkV3a6UA2i7aroNE5DlGUdQF0mQU8DQ/viewform) your preexisting
block-producing Filecoin storage-system. You will not be able to perform the API-calls below without completing this step and have a human approve your submission, usually within 2 business days. You also will not be able to request deals through this self-service system described above if your actor does not have the required minimum power, or is in a continuous faulty state.

## Workflow

0. Setup authentication you will be using to validate that you are who you say you are. Authentication is based on having access to your SP Worker Key, the same one you use to submit ProveCommits when onboarding new sectors. You can either use the reference authenticator script, or write your own [based on the simple steps described within](https://github.com/filecoin-project/evergreen-dealer/blob/master/misc/fil-spid.bash#L20-L28).

    A. Download the authenticator: `curl -OL https://raw.githubusercontent.com/filecoin-project/evergreen-dealer/master/misc/fil-spid.bash`

    B. Make it executable `chmod 755 fil-spid.bash`

    C. Use it as part of your `curl -sLH "Authorization: $( ./fil-spid.bash f0XXXX )" https://api.evergreen.filecoin.io/...`


1. Examine the lists of soon-to-expire deals currently in need for a refresh. There are 2 API endpoints for this:

    A. List of all deals eligible for renewal  contained within your own SP

    `curl -sLH "Authorization: $( ./fil-spid.bash f0xxxx )" https://api.evergreen.filecoin.io/eligible_pieces/sp_local | less`


    B. List of all deals eligible for renewal network-wide

    `curl -sLH "Authorization: $( ./fil-spid.bash f0xxxx )" https://api.evergreen.filecoin.io/eligible_pieces/anywhere | less`

    Select a set of deals you would like to renew and perform the next steps **for each of them**.

    NOTE: We recommend you start by checking the list of deals already available within your SP (option A) before proceeding network-wide, as it will be much easier to source the necessary data from your own system.


2. Request a deal proposal for each deal you selected:

    `curl -sLH "Authorization: $( ./fil-spid.bash f0xxxx )" https://api.evergreen.filecoin.io/request_piece/bagaChosenPieceCidxxxxxxxxxxxxxxx`

    From the moment of invoking this method your SP system will receive a deal proposal within ~10 minutes with a deal-start-time about 3 days (~72 hours) in the future.

3. You can view the set of outstanding deals against your SP at any time by invoking:

    `curl -sLH "Authorization: $( ./fil-spid.bash f0xxxx )" https://api.evergreen.filecoin.io/pending_proposals`

    In order to prevent abuse you can have **at most 4TiB** (128 x 32GiB sectors) outstanding against your SP at any time.

4. Repeat steps 1, 2 and 3 over and over again (you can add them to a cronjob or something similar ). In case of any difficulties or issues, don't hesitate to contact us [in #slingshot-evergreen over at the Filecoin Slack](https://filecoinproject.slack.com/archives/C0377FJCG1L)




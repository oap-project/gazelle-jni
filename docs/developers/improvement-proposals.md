---
layout: page
title: Gluten Project Improvement Proposals (GPIP)
nav_order: 10
parent: Developer Overview
---

# Gluten Project Improvement Proposals (GPIP)

The Gluten Project Improvement Proposals doc references [the Spark SPIP documentation](https://spark.apache.org/improvement-proposals.html).

The purpose of a GPIP is to inform and involve the user community in major improvements to the Gluten codebase throughout the development process to increase the likelihood that user needs are met.

GPIPs should be used for significant user-facing or cutting-edge changes, not small incremental improvements.

If your proposal meets the definition of GPIP, we recommend you to create a GPIP, which will facilitate the advancement and discussion of the proposal, but it is not mandatory, and we welcome any contribution and community participation.

## What is a GPIP?

A GPIP is similar to a product requirement document commonly used in product management.

A GPIP:

- Is a ticket labeled “GPIP” proposing a major improvement or change to Gluten
- Follows the template defined below
- Includes discussions on the ticket and dev@ list about the proposal

## Who?

Any **community member** can help by discussing whether a GPIP is likely to meet their needs and propose GPIPs.

**Contributors** can help by discussing whether a GPIP is likely to be technically feasible.

**Committers** can help by discussing whether a GPIP aligns with long-term project goals, and by shepherding GPIPs.

**GPIP Author** is any community member who authors a GPIP and is committed to pushing the change through the entire process. GPIP authorship can be transferred.

**GPIP Shepherd** is a PMC member who is committed to shepherding the proposed change throughout the entire process. Although the shepherd can delegate or work with other committers in the development process, the shepherd is ultimately responsible for the success or failure of the GPIP. Responsibilities of the shepherd include, but are not limited to:

- Be the advocate for the proposed change
- Help push forward on design and achieve consensus among key stakeholders
- Review code changes, making sure the change follows project standards
- Get feedback from users and iterate on the design & implementation
- Uphold the quality of the changes, including verifying whether the changes satisfy the goal of the GPIP and are absent of critical bugs before releasing them

## GPIP Process
### Proposing a GPIP

Anyone may propose a GPIP, using the document template below. Please only submit a GPIP if you are willing to help, at least with discussion.

After a GPIP is created, the author should email dev@gluten.apache.org to notify the community of the GPIP, and discussions should ensue on the ticket.

If a GPIP is too small or incremental and should have been done through the normal ticket process, a committer should remove the GPIP label.

### GPIP Document Template

A GPIP document is a short document with a few questions, inspired by the Heilmeier Catechism:

- Q1. What are you trying to do? Articulate your objectives using absolutely no jargon.

- Q2. What problem is this proposal NOT designed to solve?

- Q3. How is it done today, and what are the limits of current practice?

- Q4. What is new in your approach, and why do you think it will be successful?

- Q5. Who cares? If you are successful, what difference will it make?

- Q6. What are the risks?

- Q7. How long will it take?

- Q8. What are the mid-term and final “exams” to check for success?

- Appendix A. Proposed API Changes. Optional section defining APIs changes, if any. Backward and forward compatibility must be taken into account.

- Appendix B. Optional Design Sketch: How are the goals going to be accomplished? Give sufficient technical detail to allow a contributor to judge whether it's likely to be feasible. Note that this is not a full design document.

- Appendix C. Optional Rejected Designs: What alternatives were considered? Why were they rejected? If no alternatives have been considered, the problem needs more thought.

### Discussing a GPIP

All discussions of a GPIP should take place in a public forum, preferably the discussion attached to the ticket. Any discussion that happen offline should be made available online for the public via meeting notes summarizing the discussions.

During this discussion, one or more shepherds should be identified among PMC members.

Once the discussion settles, the shepherd(s) should call for a vote on the GPIP moving forward on the dev@ list. The vote should be open for at least 72 hours and follows the typical Apache vote process and passes upon consensus (at least 3 +1 votes from PMC members and no -1 votes from PMC members). dev@ should be notified of the vote result.

If there does not exist at least one PMC member that is committed to shepherding the change within a month, the GPIP is rejected.

If a committer does not think a GPIP aligns with long-term project goals, or is not practical at the point of proposal, the committer should -1 the GPIP explicitly and give technical justifications.


### Implementing a GPIP

Implementation should take place via the contribution guidelines. Changes that require GPIPs typically also require design documents to be written and reviewed.

### GPIP Community collaboration

The gluten community has always been open to contributions and participation of all kinds, and feel free to contact the community if you have any questions about the GPIP process.

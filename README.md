# ETL Logging Native, Glue, and Redshift 


## Setup Notebook
This notebook walks through a complete, hands-on setup for a Delta Lake demo using synthetic mortgage data. It begins by clearly stating the goal: generate a self-contained dataset that can be used to illustrate key Delta Lake capabilities. It then guides you through creating a realistic mortgage dataset, which serves as the foundation for all subsequent Delta operations. Finally, it shows how to materialize that data as a clustering-ready Delta table, positioning it for later demonstrations of performance optimization and advanced Delta features such as clustering and time-travel queries.

![](images/Setup-Infographic.png)


## Delta Lake Notebook
This notebook is your hands-on tour of how a Delta Lake table changes over time and how you can stay in control of it. You start by peeking into the table’s history so you can literally see each change as a versioned snapshot instead of treating the table like a black box. From there, you “time travel” into specific versions to answer questions like “What did this look like before that bad job ran?” or “What was the state right after the last batch load?” which makes debugging, audits, and reproducing old reports feel much more approachable.
​
Once you’re comfortable jumping around in time, you lean into the transactional side by running updates and deletes, watching how Delta keeps everything ACID-compliant while you reshape the data. You then finish by optimizing the table so it stays fast even after all those changes, compacting files and improving layout so queries remain snappy instead of slowing down as history grows. Put together, you end up with a realistic playbook: understand your table’s history, safely explore older versions, make targeted fixes, and then tune performance so your Delta tables stay both trustworthy and efficient as they evolve.

!j[](images/Delta-Lake-Infographic1.png)​

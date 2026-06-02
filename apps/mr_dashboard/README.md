# mr_dashboard

The **Phoenix LiveView dashboard** for Baby's First MapReduce. Part of the umbrella project — see the [top-level README](../../README.md).

It renders a real-time view of a running job: a 2D map of worker nodes (by their fictional coordinates), each worker's current task and state, and overall job progress, pushed live over WebSocket as the master and workers emit events.

You normally don't start it directly — `mix mr.start` boots the endpoint alongside the master at [`localhost:4000`](http://localhost:4000). To run the dashboard on its own (e.g. for UI work), start the Phoenix endpoint with `mix phx.server` from the umbrella root. (There is no database — this app has no Ecto repo.)

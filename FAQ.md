# FDE Interview Packet: Frequently Asked Questions

Questions below are ones a **candidate** might ask after reading [README.md](README.md) and [SETUP.md](SETUP.md). For workspace initialization, see **[SETUP.md](SETUP.md)**.

**Canonical docs for this bundle (read these first; this FAQ does not duplicate them):**

| Topic | Document |
|--------|----------|
| What's deployed, fast iteration, as-is architecture, deployment commands | [README.md](README.md) |
| Workspace setup and verify queries | [SETUP.md](SETUP.md) |

---

## Workspace Access & Setup

**Q: How do I log into the Databricks workspace?**
Credentials (username/password, SSO link, or personal access token) will be provided at the start of your prep window. Deploy and run jobs with the Databricks CLI using a named profile (for example `--profile=my-profile`). Your interviewer will provide the workspace URL and authentication method appropriate for your environment.

**Q: What catalog and schema are my tables in?**
Defaults and overrides are bundle variables — see [README.md](README.md) and [SETUP.md](SETUP.md). Confirm the actual `catalog` / `schema` with your interviewer if unsure.

**Q: Is a SQL warehouse available?**
The bundle looks up `Serverless Starter Warehouse` by default, which is present on fresh Free Edition workspaces. If your workspace uses a different warehouse, override with `--var="warehouse_id=<id>"` on deploy.

**Q: Do I have a local dev environment or only the workspace UI?**
Both. A laptop with the repo, Python, and the Databricks CLI configured for bundle commands; plus the workspace UI for browsing tables, jobs, and the dashboard. Most candidates iterate locally on Python/SQL/JSON edits and validate in the workspace via `databricks bundle deploy` and `databricks bundle run`.

---

## Working with the Scaffold

**Q: Can I modify the existing dashboard, or do I need to leave it alone?**
The dashboard is part of the as-is implementation; treat it the way you would any artifact in a customer engagement. You can extend it, replace it, or leave it in place — but own that choice in your proposal. The Lakeview JSON lives at `src/dashboard/theater_performance.lvdash.json`; redeploy with `databricks bundle deploy` to push edits.

**Q: Can I add new bundle jobs, scripts, or resources?**
Yes. The bundle is yours to extend. Add files under `src/` and register them in `resources/jobs.yml` (or a new `resources/*.yml`). `databricks bundle deploy` picks them up.

**Q: Is the seeded data deterministic? Can I safely rerun `seed_theater_data`?**
Yes. All ingest scripts use `seed=42` and write in overwrite mode, so rerunning produces the same tables. Safe to rerun any time.

---

## Building the App

**Q: How do I deploy the Databricks App?**
A basic Python application built on Databricks Apps ([docs](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/index.html)) has been provided as part of this bundle.  It is called `reel-insights-app` and will be deployed to your environment when you run `databricks bundle deploy`.  After you've deployed it you should be able to find it by navigating to the Databricks Apps UI in your workspace.  

The provided application is a Gradio app however you should feel free to modify it to use whatever framework you feel most comfortable with (Streamlit, Flask, Dash, FastAPI + frontend, etc.).

**Q: How do I start the Databricks App?**

`databricks apps start reel-insights-app --profile=my-profile`

**Q: How does the app authenticate to read tables?**
Databricks Apps run as a service principal. See the [Databricks Apps documentation](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/index.html) for the auth and permissions patterns; you'll need to grant the app's service principal access to the catalog/schema you're querying.

---

## Scope & Prioritization

**Q: 4 hours is not a lot of time. What's realistically expected?**
You are not expected to do everything in the prompt. The exercise is designed so a strong candidate can make meaningful progress on the highest-impact gap, build a credible POC, and speak credibly about what they deferred. Lead with the trade-offs you made — being upfront about prioritization is part of the evaluation.

---

## Deliverables

**Q: Is there a preferred tool for the 3-slide deck?**
No. Use whatever you're comfortable with: Google Slides, PowerPoint, a Databricks notebook in presentation mode, or even a Lakeview Dashboard. The format is less important than the content.

**Q: Is the action plan a standalone document or part of the slides?**
Either approach works. Some candidates embed it in slide 3; others create a separate artifact and reference it during the presentation. The plan must be concrete and prioritized regardless of format.

**Q: For the POC, do I create real Databricks resources or describe them?**
Both are valid in moderation, but the bias is toward something that runs. A working POC — even with stubbed components — is more compelling than a thorough description with nothing to demo.

**Q: Will panelists role-play their personas?**
Yes. Expect pointed questions from their stated perspective: the Engineering / Platform Lead will challenge feasibility and tech debt; the CIO / CDO will push on cost, timeline, and strategic fit; the VP of Theater Operations will ask whether managers will trust and adopt what you build.

---

## Logistics

**Q: Can I use the internet during the 4-hour prep?**
Yes. You may use the internet freely (Databricks documentation, Stack Overflow, blog posts, etc.). There are no restrictions on reference material. The interview is open-book.

**Q: Are there restrictions on which AI tools I can use?**
No restrictions. Use Claude, ChatGPT, Copilot, or any tool you prefer. Be prepared to explain what they produced and the decisions you made on top of their output.

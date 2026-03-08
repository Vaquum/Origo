For all the below items, add them to `1-top-level-work-plan.md` and `2-itemized-work-plan.md` and work through them as usual following all of our contracts.

- [ ] S7: Wrap everything into docker and test locally extensively to ensure that everything works as expected.
- [ ] S8: Implement OKX exchange spot trades daily data pull so that it perfectly mirrors binance/daily_trades_to_origo (https://www.okx.com/en-eu/historical-data)
- [ ] S10: Implement image-based deployment on merge (Origo repo has secrets: ORIGO_SERVER_IP, ORIGO_SERVER_PASSWORD, and ORIGO_SERVER_USER) and you have access to the info in `.env` for debugging if need to, but let the deployment install docker on the server and other required deps on first time deploy with a check if already installed or not so everything is in a single CI file (i.e. no manual setup on the server)

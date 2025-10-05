.PHONY: scrape ui


scrape:
	poetry run python scripts/run_scrape_once.py


ui:
	poetry run streamlit run apps/streamlit_app/Home.py
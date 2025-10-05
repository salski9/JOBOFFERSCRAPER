import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from scraper.settings import settings


st.set_page_config(page_title="Internship Finder (CS/AI/ML)", layout="wide")
st.title("🎓 Internship Finder – France startups & tech")


engine = create_engine(settings.DB_URL, future=True)


# Sidebar filters
q = st.sidebar.text_input("Keyword (title/desc)")
loc = st.sidebar.text_input("Location contains")
source = st.sidebar.multiselect("Sources", ["greenhouse","lever","ashby"], default=["greenhouse","lever","ashby"])
sort = st.sidebar.selectbox("Sort by", ["Newest scraped","Title A-Z"])


with engine.connect() as conn:
    rows = conn.execute(text("SELECT id, source, title, company, location, apply_url, tags, posted_at, scraped_at FROM jobs"))
    data = [dict(r._mapping) for r in rows]


df = pd.DataFrame(data)


# Quick filtering in-memory
if not df.empty:
    if q:
        ql = q.lower()
        df = df[df["title"].str.lower().str.contains(ql) | df.get("company", pd.Series("")).fillna("").str.lower().str.contains(ql)]
    if loc:
        df = df[df["location"].fillna("").str.contains(loc, case=False)]
    if source:
        df = df[df["source"].isin(source)]


if sort == "Title A-Z":
    df = df.sort_values("title", ascending=True)
else:
    df = df.sort_values("scraped_at", ascending=False)


st.caption(f"{len(df)} results")


# Display cards
for _, row in df.iterrows():
    with st.container(border=True):
        cols = st.columns([5,3,2,2])
        with cols[0]:
            st.markdown(f"**{row['title']}**")
            if row.get("company"):
                st.write(row["company"])
            if row.get("tags"):
                st.write("Tags:", ", ".join([t for t in str(row['tags']).split(',') if t]))
        with cols[1]:
            st.write(row.get("location") or "—")
        with cols[2]:
            st.write(row.get("source"))
            st.write(row.get("posted_at") or "")
        with cols[3]:
            st.link_button("Apply / View", row["apply_url"], use_container_width=True)


st.divider()
st.info("Tip: Add more companies/adapters in `scraper/pipeline/orchestrator.py` → SOURCES list, then re-run the scraper.")
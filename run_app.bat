@echo off
REM === Atalho para rodar o MVP Forttis ===
cd /d %~dp0
call .venv\Scripts\activate
python -m streamlit run app/Home.py
pause
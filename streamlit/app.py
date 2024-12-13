import os
import streamlit as st
import requests
from dotenv import load_dotenv
from datetime import datetime
from ui import planner, lesson, quiz, plans
from ui.landing_page import landing_page  # Ensure this imports the function, not the module

# Set page configuration
st.set_page_config(layout="wide", page_title="AIVY - Learning Assistant", page_icon="🔬")

# Load environment variables
load_dotenv()

# FastAPI URL from environment variables
FASTAPI_URL = os.getenv("FASTAPI_URL", "http://127.0.0.1:8000")


def user_signup(username, password):
    """
    Function to register a new user, sending a post request to the FastAPI backend.
    """
    try:
        response = requests.post(f"{FASTAPI_URL}/signup", params={"username": username, "password": password})
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        return {"detail": str(e)}


def user_login(username, password):
    """
    Function to login an existing user, verifying the user credentials through FastAPI.
    """
    try:
        response = requests.post(f"{FASTAPI_URL}/login", params={"username": username, "password": password})
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        return {"detail": str(e)}


def signup():
    st.subheader("Create Your Account 📝")

    username = st.text_input("Username", placeholder="Enter a valid username")
    password = st.text_input("Password", placeholder="Create a strong password", type="password")
    confirm_password = st.text_input("Confirm Password", placeholder="Re-enter your password", type="password")

    if st.button("Sign Up 🚀"):
        if password == confirm_password:
            result = user_signup(username, password)
            if result.get("message"):
                st.success(result["message"])
            else:
                st.error(result.get("detail", "Signup failed. Please try again."))
        else:
            st.error("Passwords do not match. Please retry.")


def login():
    st.subheader("Welcome Back 👋")

    username = st.text_input("Username", placeholder="Enter your username")
    password = st.text_input("Password", placeholder="Enter your password", type="password")

    if st.button("Login 🔑"):
        result = user_login(username, password)
        if "access_token" in result:
            st.session_state["access_token"] = result["access_token"]
            st.session_state["username"] = username
            st.session_state["logged_in"] = True
            st.session_state["page"] = "landing"
            st.experimental_rerun()
        else:
            st.error(result.get("detail", "Login failed. Please check your credentials."))


def user_logout():
    """Logs out the user and resets session state."""
    if st.button("Logout ❌", key="logout_button"):
        st.session_state.clear()
        st.experimental_rerun()


def sidebar_navigation():
    """Sidebar for navigation between pages when logged in."""
    with st.sidebar:
        st.markdown(f"### Logged in as: **{st.session_state.get('username', 'User')}** 😊")
        st.markdown("---")
        if st.button("Planner 📅"):
            change_page("planner")
        if st.button("Lessons 📚"):
            change_page("lesson")
        if st.button("Saved Plans 💾"):
            change_page("plans")
        if st.button("Quiz 🧠"):
            change_page("quiz")
        st.markdown("---")
        user_logout()


def change_page(page_name):
    """Changes the current page and triggers a rerun."""
    st.session_state["page"] = page_name
    st.experimental_rerun()


def main():
    # Initialize session state variables
    if "logged_in" not in st.session_state:
        st.session_state["logged_in"] = False

    if "page" not in st.session_state:
        st.session_state["page"] = "login"

    # Page navigation logic
    if st.session_state["logged_in"]:
        sidebar_navigation()

        if st.session_state["page"] == "landing":
            landing_page()  # Correctly calls the landing page function
        elif st.session_state["page"] == "planner":
            planner.main()
        elif st.session_state["page"] == "lesson":
            lesson.main()
        elif st.session_state["page"] == "plans":
            plans.main()
        elif st.session_state["page"] == "quiz":
            quiz.main()
        else:
            st.error("404 - Page Not Found 🚫")
    else:
        choice = st.radio("Choose an option:", ("Login", "Signup"), index=0)
        if choice == "Signup":
            signup()
        else:
            login()


if __name__ == "__main__":
    main()

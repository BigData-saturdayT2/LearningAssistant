import streamlit as st

def landing_page():
    """
    Displays the landing page with details about AIVY.
    """
    st.markdown("""
    <div style="padding: 20px; text-align: center;">
        <h2>About AIVY</h2>
        <p>AIVY is a next-generation AI-powered learning assistant that empowers users to:</p>
        <ul style="text-align: left; display: inline-block;">
            <li>Plan and organize their study schedules using an interactive planner.</li>
            <li>Access personalized lessons tailored to individual learning goals.</li>
            <li>Save and manage study plans efficiently.</li>
            <li>Test knowledge and track progress with interactive quizzes.</li>
        </ul>
        <p>AIVY is designed to make learning efficient, engaging, and personalized for everyone.</p>
        <button onclick="window.location.href='/?page=planner'" style="padding: 10px 20px; font-size: 16px; background-color: #4CAF50; color: white; border: none; border-radius: 5px; cursor: pointer;">Go to Planner</button>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    st.set_page_config(page_title="AIVY - About", layout="centered")
    landing_page()
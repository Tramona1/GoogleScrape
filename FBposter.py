import os
import time
import logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration ---
GROUP_URL = "https://www.facebook.com/groups/GatlinburgVacationRentals"
POST_TEXT = "Hello, I'm looking to travel to the area for the first time ever. Anyone have any recommendations on places to stay or visit?"
FACEBOOK_EMAIL_OR_PHONE = "YOUR_FACEBOOK_EMAIL_OR_PHONE"  # Replace with your Facebook login email or phone
FACEBOOK_PASSWORD = "YOUR_FACEBOOK_PASSWORD" # Replace with your Facebook password

# --- Chrome Options ---
chrome_options = Options()
chrome_options.add_argument("--remote-debugging-port=9222")
chrome_options.add_argument("--log-level=3")
chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
chrome_options.add_argument(f"--user-data-dir={os.path.expanduser('~')}/fb-selenium-profile") # Reuse existing session
chrome_options.add_argument("--profile-directory=Default") # Default profile directory
chrome_options.add_argument("--disable-blink-features=AutomationControlled") # Stability
chrome_options.add_argument("--no-default-browser-check") # Stability

# --- Initialize WebDriver ---
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)

def is_logged_in(driver):
    try:
        driver.find_element(By.XPATH, "//div[@role='button' and .//span[text()='Write something...']]")
        return True
    except NoSuchElementException:
        return False

def login_facebook(driver, email, password):
    logging.info("Attempting to log in to Facebook...")
    try:
        email_field = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.NAME, "email"))
        )
        password_field = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.NAME, "pass"))
        )
        login_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.NAME, "login"))
        )

        email_field.send_keys(email)
        password_field.send_keys(password)
        login_button.click()
        logging.info("Login button clicked.")

        logging.info("Waiting for manual 2FA completion. Please complete 2FA in the browser now...") # <---- Log message for 2FA wait
        # Wait for login to complete (INCREASED TIMEOUT FOR 2FA)
        WebDriverWait(driver, 300).until(  # <----- INCREASED TIMEOUT TO 300 SECONDS (5 MINUTES)
            EC.url_contains("facebook.com") # Basic check, improve if needed
        )
        if is_logged_in(driver):
            logging.info("Login successful.")
            return True
        else:
            logging.warning("Login might have failed, not redirected to logged-in page after login button click.")
            return False


    except TimeoutException:
        logging.error("Timeout during login process - could not find login elements or login took too long.")
        return False
    except Exception as e:
        logging.error(f"An error occurred during login: {e}")
        return False


try:
    # Step 1: Navigate to Facebook Group URL
    logging.info("Navigating to group page...")
    driver.get(GROUP_URL)

    # Step 2: Check Login Status and Login if Necessary
    if not is_logged_in(driver):
        logging.info("User is not logged in. Initiating Facebook login...")
        if not login_facebook(driver, FACEBOOK_EMAIL_OR_PHONE, FACEBOOK_PASSWORD):
            raise Exception("Facebook login failed. Please check credentials and try again.")
    else:
        logging.info("User is already logged in.")

    # Step 3: Click "Write something..." Button (to open post editor)
    logging.info("Waiting for 'Write something...' button...")
    write_box = WebDriverWait(driver, 20).until(
        EC.element_to_be_clickable((By.XPATH, "//div[@role='button' and .//span[text()='Write something...']]"))
    )
    write_box.click()
    logging.info("'Write something...' button clicked.")

    logging.info("Pausing for manual inspection for 60 seconds. Please inspect the element now.") # ADDED PAUSE FOR INSPECTION
    time.sleep(60)  # <----- ADD THIS PAUSE FOR INSPECTION
    # time.sleep(5) # Allow post editor to load  <--- Comment out or remove the shorter sleep


    # Step 4: Enter Post Text in Textbox
    logging.info("Waiting for post textbox...")

    # --- Locator Strategy: XPath with aria-describedby ---
    post_field_locator = (By.XPATH, "//div[@role='textbox' and @aria-describedby='placeholder-edukr']")
    logging.info(f"Using post textbox locator: {post_field_locator}") # Log the locator

    try:
        # --- Robust Wait Sequence with Detailed Logging ---
        logging.info("Waiting for presence_of_element_located...")
        WebDriverWait(driver, 60).until(
            EC.presence_of_element_located(post_field_locator)
        )
        logging.info("presence_of_element_located: Element found in DOM.")

        logging.info("Waiting for visibility_of_element_located...")
        WebDriverWait(driver, 60).until(
            EC.visibility_of_element_located(post_field_locator)
        )
        logging.info("visibility_of_element_located: Element is visible.")

        logging.info("Waiting for element_to_be_clickable...")
        post_field = WebDriverWait(driver, 60).until(
            EC.element_to_be_clickable(post_field_locator)
        )
        logging.info("element_to_be_clickable: Element is clickable.")

        post_field.click() # Explicitly click
        post_field.send_keys(POST_TEXT)
        logging.info("Post text entered.")
        time.sleep(3) # Pause after entering text

    except TimeoutException as te:
        logging.error(f"TimeoutException during WebDriverWait: {te}")
        logging.error(f"Current URL: {driver.current_url}") # Log URL in case of redirect
        logging.error("Attempting to find elements using driver.find_elements() to debug...")
        elements_found = driver.find_elements(*post_field_locator) # Find all matching elements (even if not visible/clickable)
        if elements_found:
            logging.info(f"driver.find_elements() found {len(elements_found)} elements matching locator, but WebDriverWait failed.")
            for element in elements_found:
                logging.info(f"Element (tag name: {element.tag_name}, class: {element.get_attribute('class')}, aria-label: {element.get_attribute('aria-label')}, role: {element.get_attribute('role')}, visible: {element.is_displayed()}, enabled: {element.is_enabled()})") # Log element details
        else:
            logging.info("driver.find_elements() found NO elements matching locator.") # Log if no elements found

    except Exception as wait_e: # Catch other potential WebDriverWait exceptions
        logging.error(f"WebDriverWait Exception: {wait_e}")

    # Step 5: Click "Post" Button to Submit Post
    logging.info("Waiting for 'Post' button...")
    post_button_locator = (By.XPATH, "//div[@role='button']//span[text()='Post']") # Refined locator
    post_button = WebDriverWait(driver, 20).until(
        EC.element_to_be_clickable(post_button_locator)
    )
    post_button.click()
    logging.info("'Post' button clicked.")

    # Step 6: Wait After Posting (to ensure it completes)
    time.sleep(10)
    logging.info("Post published successfully.")


except Exception as e:
    logging.error(f"An error occurred: {e}")

finally:
    driver.quit()
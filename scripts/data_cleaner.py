import pandas as pd
import re
import os
import emoji
from logger import Logger

class DataCleaner:
    """
    A class to clean and preprocess Telegram data from a CSV file.
    
    Attributes:
        CHANNEL_USERNAME (str): Column name for channel usernames.
        MESSAGE (str): Column name for messages.
        DATE (str): Column name for dates.
        ID (str): Column name for unique identifiers.
        Media_path (str): Column name for media paths.
        logger (Logger): Custom logger instance for logging messages.
        allowed_characters (RegexPattern): Regex pattern to allow specific characters in messages.
    """
    CHANNEL_ID = 'channel_id'
    CHANNEL_TITLE = 'Channel Title'
    CHANNEL_USERNAME = 'Channel Username'
    MESSAGE = 'Message'
    DATE = 'Date'
    ID = 'ID'  # Updated to match the column name from the CSV
    Media_path = 'Media Path'
    
    def __init__(self):
        """
        Initializes the DataCleaner with a custom logger instance and allowed character patterns.
        """
        self.logger = Logger(log_file='../data/cleaner_log.log')
        self.allowed_characters = re.compile(r'[^a-zA-Z0-9\s.,!?;:()[]@&]+')

    def load_data(self, file_path):
        """
        Loads data from a CSV file into a pandas DataFrame.

        Args:
            file_path (str): The path to the CSV file to be loaded.

        Returns:
            pd.DataFrame: DataFrame containing the loaded data, or an empty DataFrame if loading fails.
        """
        try:
            data = pd.read_csv(file_path)
            self.logger.info(f"Data loaded successfully. Shape: {data.shape}")
            return data
        except FileNotFoundError:
            self.logger.error("File not found. Please check the file path.")
            return pd.DataFrame()  # Return empty DataFrame for consistency
        except Exception as e:
            self.logger.error(f"An error occurred while loading data: {str(e)}")
            return pd.DataFrame()

    def remove_duplicates(self, data, image_directory):
        """
        Removes duplicate entries from the DataFrame and corresponding images from the specified directory.

        Args:
            data (pd.DataFrame): DataFrame from which duplicates are to be removed.
            image_directory (str): Directory containing images corresponding to messages.

        Returns:
            pd.DataFrame: DataFrame without duplicates.
        """
        duplicates = data[data.duplicated(subset=self.ID, keep='first')]
        data = data.drop_duplicates(subset=self.ID, keep='first')
        self.logger.info(f"Duplicates removed. New shape: {data.shape}")
        self._remove_duplicate_images(duplicates, image_directory)
        return data

    def _remove_duplicate_images(self, duplicates, image_directory):
        """
        Removes images that correspond to duplicate entries.

        Args:
            duplicates (pd.DataFrame): DataFrame containing duplicate entries.
            image_directory (str): Directory where images are stored.
        """
        for index, row in duplicates.iterrows():
            channel_username = row[self.CHANNEL_USERNAME]
            message_id = row[self.ID]
            image_name = f"{channel_username}_{message_id}.jpg"
            image_path = os.path.join(image_directory, image_name)

            if os.path.exists(image_path):
                try:
                    os.remove(image_path)
                    self.logger.info(f"Removed duplicate image: {image_path}")
                except Exception as e:
                    self.logger.error(f"Error removing image: {image_path}. Exception: {str(e)}")

    def handle_missing_values(self, data):
        """
        Handles missing values in the DataFrame by filling them with placeholders.

        Args:
            data (pd.DataFrame): DataFrame in which missing values are to be handled.

        Returns:
            pd.DataFrame: DataFrame with missing values filled.
        """
        data.fillna({
            self.CHANNEL_USERNAME: 'Unknown',
            self.MESSAGE: 'N/A',
            self.DATE: '1970-01-01 00:00:00'
        }, inplace=True)
        self.logger.info("Missing values handled.")
        return data

    def standardize_formats(self, data):
        """
        Standardizes the formats of specific columns in the DataFrame.

        Args:
            data (pd.DataFrame): DataFrame containing the data to be standardized.

        Returns:
            pd.DataFrame: DataFrame with standardized formats.
        """
        # Convert Date column to datetime
        if self.DATE in data.columns:
            data[self.DATE] = pd.to_datetime(data[self.DATE], errors='coerce')

        # Clean and format message content
        if self.MESSAGE in data.columns:
            data[self.MESSAGE] = data[self.MESSAGE].apply(self.clean_message_content).str.lower().str.strip()

        # Clean and format channel names
        if self.CHANNEL_USERNAME in data.columns:
            data[self.CHANNEL_USERNAME] = data[self.CHANNEL_USERNAME].str.replace(r'[^a-zA-Z0-9\s]', '', regex=True).str.strip().str.title()

        self.logger.info("Formats standardized.")
        return data

    def clean_message_content(self, text):
        """
        Cleans the message content by removing unwanted characters, including emojis.

        Args:
            text (str): The message content to be cleaned.

        Returns:
            str: The cleaned message content.
        """
        # Remove emojis
        text = emoji.replace_emoji(text, replace='')  # Remove emojis
        # Remove unwanted characters but keep specific patterns intact
        text = re.sub(self.allowed_characters, '', text)  # Remove unwanted characters
        # Remove extra whitespace (including tabs and newlines)
        text = re.sub(r'\s+', ' ', text)  # Replace multiple spaces with a single space
        
        return text.strip()

    def validate_data(self, data):
        """
        Validates the cleaned data for inconsistencies and removes invalid entries.

        Args:
            data (pd.DataFrame): DataFrame to validate.

        Returns:
            pd.DataFrame: Validated DataFrame with inconsistencies removed.
        """
        # Drop rows with invalid Dates
        data = data.dropna(subset=[self.DATE])

        # Validate message content length
        if self.MESSAGE in data.columns:
            data = data[data[self.MESSAGE].str.len() <= 1000]

        # Validate channel names
        data = data[data[self.CHANNEL_USERNAME].str.len() > 0]

        self.logger.info("Data validation completed.")
        return data

    def save_cleaned_data(self, data, file_path):
        """
        Saves the cleaned data to a CSV file with standardized SQL column names.

        Args:
            data (pd.DataFrame): DataFrame containing the cleaned data.
            file_path (str): Original path of the CSV file to determine save location.
        """
        # Define the mapping of raw column names to SQL-friendly names
        column_mapping = {
            self.CHANNEL_ID: 'channel_id',
            self.CHANNEL_USERNAME: 'channel_username',
            self.CHANNEL_TITLE: 'channel_title',
            self.ID: 'message_id',
            self.MESSAGE: 'Message',
            self.DATE: 'date',
            self.Media_path: 'media_path'
        }
        
        # Rename columns in the DataFrame
        data.rename(columns=column_mapping, inplace=True)

        # Create the cleaned file path
        cleaned_file_path = file_path.replace('.csv', '_cleaned.csv')
        data.to_csv(cleaned_file_path, index=False)
        self.logger.info(f"Cleaned data saved to {cleaned_file_path}.")

    def clean_telegram_data(self, file_path, image_directory):
        """
        Main function to clean Telegram data stored in a CSV file and remove corresponding duplicate images.

        Args:
            file_path (str): The path to the CSV file containing Telegram data.
            image_directory (str): The directory path where images are stored.

        Returns:
            pd.DataFrame: A cleaned pandas DataFrame, or an empty DataFrame if cleaning fails.
        """
        try:
            # Load the data
            data = self.load_data(file_path)
            if data.empty:
                self.logger.error("No data loaded, cleaning process aborted.")
                return data
            
            # Run cleaning steps
            data = self.remove_duplicates(data, image_directory)
            data = self.handle_missing_values(data)
            data = self.standardize_formats(data)
            data = self.validate_data(data)
            
            # Save cleaned data
            self.save_cleaned_data(data, file_path)
            
            self.logger.info("Data cleaning completed successfully.")
            return data

        except Exception as e:
            self.logger.error(f"An error occurred during the cleaning process: {str(e)}")
            return pd.DataFrame()

# Run the function
if __name__ == '__main__':
    # Class instance
    cleaner = DataCleaner()
    # Call the main telegram cleaner function
    cleaner.clean_telegram_data('../data/telegram_data.csv', '../data/photos/')
import pandas as pd
import re
import os
import emoji
from logger import Logger

class DataCleaner:
    """
    A class to clean and preprocess Telegram data from a CSV file.
    
    Attributes:
        CHANNEL_USERNAME (str): Column name for channel usernames.
        MESSAGE (str): Column name for messages.
        DATE (str): Column name for dates.
        ID (str): Column name for unique identifiers.
        Media_path (str): Column name for media paths.
        logger (Logger): Custom logger instance for logging messages.
        allowed_characters (RegexPattern): Regex pattern to allow specific characters in messages.
    """
    CHANNEL_ID = 'channel_id'
    CHANNEL_TITLE = 'Channel Title'
    CHANNEL_USERNAME = 'Channel Username'
    MESSAGE = 'Message'
    DATE = 'Date'
    ID = 'ID'  # Updated to match the column name from the CSV
    Media_path = 'Media Path'
    
    def __init__(self):
        """
        Initializes the DataCleaner with a custom logger instance and allowed character patterns.
        """
        self.logger = Logger(log_file='../data/cleaner_log.log')
        self.allowed_characters = re.compile(r'[^a-zA-Z0-9\s.,!?;:()[]@&]+')

    def load_data(self, file_path):
        """
        Loads data from a CSV file into a pandas DataFrame.

        Args:
            file_path (str): The path to the CSV file to be loaded.

        Returns:
            pd.DataFrame: DataFrame containing the loaded data, or an empty DataFrame if loading fails.
        """
        try:
            data = pd.read_csv(file_path)
            self.logger.info(f"Data loaded successfully. Shape: {data.shape}")
            return data
        except FileNotFoundError:
            self.logger.error("File not found. Please check the file path.")
            return pd.DataFrame()  # Return empty DataFrame for consistency
        except Exception as e:
            self.logger.error(f"An error occurred while loading data: {str(e)}")
            return pd.DataFrame()

    def remove_duplicates(self, data, image_directory):
        """
        Removes duplicate entries from the DataFrame and corresponding images from the specified directory.

        Args:
            data (pd.DataFrame): DataFrame from which duplicates are to be removed.
            image_directory (str): Directory containing images corresponding to messages.

        Returns:
            pd.DataFrame: DataFrame without duplicates.
        """
        duplicates = data[data.duplicated(subset=self.ID, keep='first')]
        data = data.drop_duplicates(subset=self.ID, keep='first')
        self.logger.info(f"Duplicates removed. New shape: {data.shape}")
        self._remove_duplicate_images(duplicates, image_directory)
        return data

    def _remove_duplicate_images(self, duplicates, image_directory):
        """
        Removes images that correspond to duplicate entries.

        Args:
            duplicates (pd.DataFrame): DataFrame containing duplicate entries.
            image_directory (str): Directory where images are stored.
        """
        for index, row in duplicates.iterrows():
            channel_username = row[self.CHANNEL_USERNAME]
            message_id = row[self.ID]
            image_name = f"{channel_username}_{message_id}.jpg"
            image_path = os.path.join(image_directory, image_name)

            if os.path.exists(image_path):
                try:
                    os.remove(image_path)
                    self.logger.info(f"Removed duplicate image: {image_path}")
                except Exception as e:
                    self.logger.error(f"Error removing image: {image_path}. Exception: {str(e)}")

    def handle_missing_values(self, data):
        """
        Handles missing values in the DataFrame by filling them with placeholders.

        Args:
            data (pd.DataFrame): DataFrame in which missing values are to be handled.

        Returns:
            pd.DataFrame: DataFrame with missing values filled.
        """
        data.fillna({
            self.CHANNEL_USERNAME: 'Unknown',
            self.MESSAGE: 'N/A',
            self.DATE: '1970-01-01 00:00:00'
        }, inplace=True)
        self.logger.info("Missing values handled.")
        return data

    def standardize_formats(self, data):
        """
        Standardizes the formats of specific columns in the DataFrame.

        Args:
            data (pd.DataFrame): DataFrame containing the data to be standardized.

        Returns:
            pd.DataFrame: DataFrame with standardized formats.
        """
        # Convert Date column to datetime
        if self.DATE in data.columns:
            data[self.DATE] = pd.to_datetime(data[self.DATE], errors='coerce')

        # Clean and format message content
        if self.MESSAGE in data.columns:
            data[self.MESSAGE] = data[self.MESSAGE].apply(self.clean_message_content).str.lower().str.strip()

        # Clean and format channel names
        if self.CHANNEL_USERNAME in data.columns:
            data[self.CHANNEL_USERNAME] = data[self.CHANNEL_USERNAME].str.replace(r'[^a-zA-Z0-9\s]', '', regex=True).str.strip().str.title()

        self.logger.info("Formats standardized.")
        return data

    def clean_message_content(self, text):
        """
        Cleans the message content by removing unwanted characters, including emojis.

        Args:
            text (str): The message content to be cleaned.

        Returns:
            str: The cleaned message content.
        """
        # Remove emojis
        text = emoji.replace_emoji(text, replace='')  # Remove emojis
        # Remove unwanted characters but keep specific patterns intact
        text = re.sub(self.allowed_characters, '', text)  # Remove unwanted characters
        # Remove extra whitespace (including tabs and newlines)
        text = re.sub(r'\s+', ' ', text)  # Replace multiple spaces with a single space
        
        return text.strip()

    def validate_data(self, data):
        """
        Validates the cleaned data for inconsistencies and removes invalid entries.

        Args:
            data (pd.DataFrame): DataFrame to validate.

        Returns:
            pd.DataFrame: Validated DataFrame with inconsistencies removed.
        """
        # Drop rows with invalid Dates
        data = data.dropna(subset=[self.DATE])

        # Validate message content length
        if self.MESSAGE in data.columns:
            data = data[data[self.MESSAGE].str.len() <= 1000]

        # Validate channel names
        data = data[data[self.CHANNEL_USERNAME].str.len() > 0]

        self.logger.info("Data validation completed.")
        return data

    def save_cleaned_data(self, data, file_path):
        """
        Saves the cleaned data to a CSV file with standardized SQL column names.

        Args:
            data (pd.DataFrame): DataFrame containing the cleaned data.
            file_path (str): Original path of the CSV file to determine save location.
        """
        # Define the mapping of raw column names to SQL-friendly names
        column_mapping = {
            self.CHANNEL_ID: 'channel_id',
            self.CHANNEL_USERNAME: 'channel_username',
            self.CHANNEL_TITLE: 'channel_title',
            self.ID: 'message_id',
            self.MESSAGE: 'Message',
            self.DATE: 'date',
            self.Media_path: 'media_path'
        }
        
        # Rename columns in the DataFrame
        data.rename(columns=column_mapping, inplace=True)

        # Create the cleaned file path
        cleaned_file_path = file_path.replace('.csv', '_cleaned.csv')
        data.to_csv(cleaned_file_path, index=False)
        self.logger.info(f"Cleaned data saved to {cleaned_file_path}.")

    def clean_telegram_data(self, file_path, image_directory):
        """
        Main function to clean Telegram data stored in a CSV file and remove corresponding duplicate images.

        Args:
            file_path (str): The path to the CSV file containing Telegram data.
            image_directory (str): The directory path where images are stored.

        Returns:
            pd.DataFrame: A cleaned pandas DataFrame, or an empty DataFrame if cleaning fails.
        """
        try:
            # Load the data
            data = self.load_data(file_path)
            if data.empty:
                self.logger.error("No data loaded, cleaning process aborted.")
                return data
            
            # Run cleaning steps
            data = self.remove_duplicates(data, image_directory)
            data = self.handle_missing_values(data)
            data = self.standardize_formats(data)
            data = self.validate_data(data)
            
            # Save cleaned data
            self.save_cleaned_data(data, file_path)
            
            self.logger.info("Data cleaning completed successfully.")
            return data

        except Exception as e:
            self.logger.error(f"An error occurred during the cleaning process: {str(e)}")
            return pd.DataFrame()

# Run the function
if __name__ == '__main__':
    # Class instance
    cleaner = DataCleaner()
    # Call the main telegram cleaner function
    cleaner.clean_telegram_data('../data/telegram_data.csv', '../data/photos/')
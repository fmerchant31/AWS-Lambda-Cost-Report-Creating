import copy
from datetime import date, datetime, timedelta

import boto3
import pandas as pd
from dateutil.relativedelta import relativedelta

from json_logger import logger

# Set CSS properties for th elements in dataframe
th_props = [
    ('font-size', '12px'),
    ('text-align', 'justify'),
    ('font-weight', 'bold'),
    ('color', '#6d6d6d'),
    ('background-color', '#f7f7f9'),
    ('padding', '5px')
]

# Set CSS properties for td elements in dataframe
td_props = [
    ('font-size', '12px'),
    ('padding', '10px')
]

# Set table style
styles = [
    dict(selector="th", props=th_props),
    dict(selector="td", props=td_props)
]


def color_cell(cell):
    """
    Return cell styling based on value of cell
    Red if negative
    Green if positive or zero
    """
    return 'color: ' + ('red' if cell > 0 else 'green')


def get_timeperiod(start: date, end: date) -> dict:
    """
    Generate time period in ISO format
    Args:
        start:
        end:

    Returns:

    """
    return {
        "Start": start.isoformat(),
        "End": end.isoformat(),
    }


class CostExplorer:
    """
    Class containing all function related to Costexplorer API
    """
    def __init__(self):
        """
        Initialise the client and set some class variables
        """
        today_date = datetime.utcnow().date()
        first_day_of_current_month = today_date.replace(day=1)

        # Eg. If today is 2021-09-01, then to get data set from August, we set first of current month to 2021-08-01
        if today_date == first_day_of_current_month:
            first_day_of_current_month = first_day_of_current_month - relativedelta(months=+1)
        first_day_of_previous_month = first_day_of_current_month - relativedelta(months=+1)
        self.second_day_of_month = True if today_date.day == 2 else False

        # Relative delta handles conditions for varying month lengths.
        # Eg. If today is 2021-03-31, then self.relative_prev_month_date would evaluate to 2021-02-28 and to 29 for leap years
        relative_prev_month_date: date = today_date - relativedelta(months=+1)

        self.logger = logger

        self.client = boto3.client("ce")
        metrics = ["UNBLENDED_COST"]

        # Filter to get daily data from beginning of previous month to relative date from previous month (exclusive)
        self.daily_report_previous_month_kwargs = {
            "TimePeriod": get_timeperiod(

                start=first_day_of_previous_month,  # start date is inclusive
                end=relative_prev_month_date,  # end date is exclusive
            ),
            "Metrics": metrics,
            "Granularity": "DAILY",
            "GroupBy": [
                {
                    "Key": "SERVICE",
                    "Type": "DIMENSION"
                }
            ]
        }

        # Filter to get daily data from beginning of current month to today (exclusive)
        self.daily_report_current_month_kwargs = {
            "TimePeriod": get_timeperiod(
                # Need minimum two elements in current month list to calculate DoD diff. Hence, if today is the second
                # of the month, we set the start date to last day of previous month. Range becomes last day of prev
                # month to today (second). Eg. if today is 2021-09-02, range becomes 2021-08-31 to 2021-09-02, which
                # gets us the elements 2021-08-31 to 2021-08-01 and 2021-09-01 to 2021-09-02
                start=first_day_of_current_month - relativedelta(days=+1) if today_date.day == 2 else first_day_of_current_month, # start date is inclusive
                end=today_date,  # end date is exclusive
            ),
            "Metrics": metrics,
            "Granularity": "DAILY",
            "GroupBy": [
                {
                    "Key": "SERVICE",
                    "Type": "DIMENSION"
                }
            ]
        }
        self.total_cost_this_month = 'Total cost this month'
        self.total_cost_last_month = 'Total cost last month'
        self.mom_cost_diff = 'MoM Cost Diff'
        self.dod_cost_diff = 'DoD Cost Diff'
        

    def generate_report(self) -> str:
        """
        Generate daily cost report
        Returns: HTML table as string

        """
        current_month_response = self.client.get_cost_and_usage(**self.daily_report_current_month_kwargs)
        self.logger.info("Current month's response", extra=dict(data=current_month_response))
        previous_month_response = self.client.get_cost_and_usage(**self.daily_report_previous_month_kwargs)
        self.logger.info("Previous month's response", extra=dict(data=previous_month_response))

        return self.format_as_table(current_month_response, previous_month_response)

    def format_as_table(self, current_month_response: dict, previous_month_response: dict) -> str:
        """
        Format the response into a table using pandas dataframe
        Args:
            current_month_response:
            previous_month_response:

        Returns: Stylised dataframe as HTML string

        """
        service_cost_list_curr_day = current_month_response['ResultsByTime'][-1]['Groups']
        service_cost_list_prev_day = current_month_response['ResultsByTime'][-2]['Groups']

        current_day_date = current_month_response['ResultsByTime'][-1]['TimePeriod']['Start']
        previous_day_date = current_month_response['ResultsByTime'][-2]['TimePeriod']['Start']

        # Define the list objects will be the columns in the dataframe
        service_name_list, curr_day_cost_list, prev_day_cost_list, total_cost_till_date_current_month = [], [], [], []
        total_cost_till_relative_date_previous_month = []

        for service in service_cost_list_curr_day:
            # Build first column with list of services from the list of service cost list for current day
            service_name = service.get('Keys')[0]
            service_name_list.append(service_name)

            # Build column data for current day's cost
            curr_day_cost_list.append(service.get('Metrics').get('UnblendedCost').get('Amount'))

            # Build column data for previous day's cost
            previous_day_service_cost = 0.0
            # Iterate over previous day's cost list and find corresponding values for service_name
            for prev_service in service_cost_list_prev_day:
                if prev_service.get('Keys')[0] == service_name:
                    previous_day_service_cost = prev_service.get('Metrics').get('UnblendedCost').get('Amount')
                    break
            prev_day_cost_list.append(previous_day_service_cost)

            # Build column data for total cost in the current month for a service
            total_service_cost_current_month = self.calculate_monthly_cost(current_month_response, service_name, "current")
            total_cost_till_date_current_month.append(total_service_cost_current_month)

            # Build column data for total cost in the previous month for a service
            total_service_cost_previous_month = self.calculate_monthly_cost(previous_month_response, service_name, "previous")
            total_cost_till_relative_date_previous_month.append(total_service_cost_previous_month)

        # Add lists (columns) to data object where keys are column names and values for the row elements of the column
        data = {"Service Name": service_name_list, self.total_cost_this_month: total_cost_till_date_current_month,
                f'Cost on {current_day_date}': curr_day_cost_list, previous_day_date: prev_day_cost_list,
                self.total_cost_last_month: total_cost_till_relative_date_previous_month}

        df = self.create_dataframe(data, current_day_date, previous_day_date)

        # Render DF as HTML with float set to 2 decimal places and apply color to cells in DoD Cost Diff and
        # MoM Cost Diff based on their value, and also add table style
        html_table = df.style.format(precision=2).applymap(color_cell,
                                                           subset=[self.dod_cost_diff, self.mom_cost_diff]).set_table_styles(
            styles).render()
        return html_table

    def calculate_monthly_cost(self, cost_response_object: dict, service_name: str, month: str) -> float:
        """
        Calculate the total cost for a service from a cost_response_object
        Args:
            month:
            cost_response_object:
            service_name:

        Returns:
            The SUM total cost of a service based on values in cost_response_object

        """
        total_service_cost_in_month = 0.0

        # If second day of the current month, then first element in result_list will be the cost for
        # last day of previous month (used earlier to calculate DoD diff), which is not needed in monthly cost
        if month == "current" and self.second_day_of_month:
            new_response_obj = copy.deepcopy(cost_response_object)
            result_list = new_response_obj['ResultsByTime']
            result_list.pop(0)
        else:
            result_list = cost_response_object['ResultsByTime']
        for each_day in result_list:
            # Inner loop - iterate over service list to find the object Metrics object which contains cost
            # of the service_name
            for item in each_day['Groups']:
                if item.get('Keys')[0] == service_name:
                    total_service_cost_in_month = total_service_cost_in_month + float(
                        item.get('Metrics').get('UnblendedCost').get('Amount'))
                    # Break out of inner loop when service is found
                    break
        return total_service_cost_in_month

    def create_dataframe(self, data: dict, current_day_date: str, previous_day_date: str) -> pd.DataFrame:
        """
        Create a dataframe object as per spec
        Args:
            previous_day_date:
            current_day_date:
            data:

        Returns:

        """
        # Create a 2D object (dataframe) using the data dictionary
        df = pd.DataFrame(data=data)

        # List of columns which are to be converted from string to numeric
        columns_to_numeric = [self.total_cost_this_month, f'Cost on {current_day_date}', previous_day_date,
                              self.total_cost_last_month]
        for column in columns_to_numeric:
            # Convert column to numeric data type
            df[column] = pd.to_numeric(df[column])

        # Add column which contains Day over Day cost difference
        df['DoD Cost Diff'] = df[f'Cost on {current_day_date}'] - df[previous_day_date]

        # Drop the previous day's cost column as it's no longer needed
        df.drop([previous_day_date], axis=1, inplace=True)

        # Add column which contains Month over Month cost difference
        df[self.mom_cost_diff] = df[self.total_cost_this_month] - df[self.total_cost_last_month]

        # Sort dataframe by the values in "MoM Cost Diff" column
        df.sort_values(by=[self.mom_cost_diff], inplace=True)

        # Construct a list of columns with numeric data types which are to be rounded to two decimal places
        round_columns_titles = [self.total_cost_this_month, self.total_cost_last_month, self.mom_cost_diff,
                                f'Cost on {current_day_date}', self.dod_cost_diff]
        df[round_columns_titles] = df[round_columns_titles].round(2)

        # After rounding some values show up as -0.00. Update them to 0.00.
        df.replace(-0.00, 0.00, inplace=True)

        # Make "Service Name" column as the index
        updated_df = df.set_index('Service Name')

        # Add a row which contains the sum of all rows
        updated_df.loc['Total'] = updated_df.sum(numeric_only=True)

        # Change order of columns to the match the list: round_columns_titles
        updated_df = updated_df.reindex(columns=round_columns_titles)
        return updated_df

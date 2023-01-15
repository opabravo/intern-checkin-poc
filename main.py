import gspread
import pytz
import re
import os
import sys
import json
import argparse
from loguru import logger
from pathlib import Path
from datetime import datetime, timedelta


@logger.catch
def select_worksheet(spread_sheet: gspread.Spreadsheet, date: datetime) -> gspread.Worksheet:
    """Select the worksheet depending on date"""
    year, month, day = date.year, date.month, date.day
    # Worksheet will be named to next month if current date is above 21th
    if day >= 21:
        # If current month is December, move to next year
        if month == 12:
            month = 1
            year += 1
        else:
            month += 1
    month = f"{month}" if month > 9 else f"0{month}"
    sheet_name = f"{year}{month}"
    worksheeet = spread_sheet.worksheet(sheet_name)
    logger.debug({worksheeet})
    return worksheeet


def get_adjusted_checkout_time(check_in_time: datetime, check_out_time: datetime):
    """Get adjusted checkout time"""
    hours = check_out_time.hour - check_in_time.hour
    minutes = check_out_time.minute - check_in_time.minute
    elapsed_hours = hours + minutes / 60
    # Once we reach >= X.3, hour, it will be rounded up to X.5
    hour_i, hour_d = divmod(elapsed_hours, 1)
    adjsuted_hours = hour_i + hour_d
    if 0.3 <= hour_d < 0.5:
        adjsuted_hours = hour_i+0.5
    if hour_d >= 0.8:
        adjsuted_hours = hour_i+1
    return check_in_time + timedelta(hours=adjsuted_hours)


def get_cells_to_update(worksheet: gspread.Worksheet, employee_id: int, date: datetime) -> tuple:
    """Get cells to update by employee id and date"""
    search_regex = re.compile(rf"{employee_id}-")
    user_row = worksheet.find(search_regex, in_column=1)
    if not user_row:
        logger.error(f"Employee : {employee_id} not found")
        return
    # Get today's column
    if os.name == "nt":
        date_str = date.strftime("%#m/%#d")
    else:
        date_str = date.strftime("%-m/%-d")

    today_col = worksheet.find(date_str, in_row=1)
    if not today_col:
        logger.error(f"Date : {date_str} not found")
        return
    logger.debug(
        f"Matched {search_regex} at row {user_row}, column {today_col}")
    return user_row, today_col


def get_check_in_sheet(sheet_key: str) -> gspread.Spreadsheet:
    try:
        check_in_sheet = gc.open_by_key(sheet_key)
    except gspread.exceptions.APIError as e:
        logger.error(f"Error while loading Spreadsheet: {e}")
    else:
        logger.debug(f"Spreadsheet opened : {check_in_sheet}")
        return check_in_sheet


def load_config() -> dict:
    """Load config from config.json"""
    config_file = Path("config.json")
    if not config_file.exists():
        logger.error("Config file not found")
        sheet_key = input("\nCheckin Google Sheeet key(in URL): ")
        employee_id = input("Employee ID: ")
        save_config({"checkin_sheet_key": sheet_key,
                    "employee_id": employee_id})
    try:
        with open(config_file, "r") as f:
            config = json.load(f)
    except json.decoder.JSONDecodeError as e:
        logger.exception(f"Error while loading config: {e}")
    else:
        logger.debug(f"Config loaded : {config}")
        return config


def save_config(config: dict):
    """Save config to config.json"""
    config_file = Path("config.json")
    try:
        with open(config_file, "w") as f:
            json.dump(config, f, indent=4)
    except Exception as e:
        logger.exception(f"Error while saving config: {e}")
    else:
        logger.debug(f"Config saved : {config}")


@logger.catch
def main():
    config = load_config()
    if not config:
        return
    check_in_sheet = get_check_in_sheet(config["checkin_sheet_key"])
    if not check_in_sheet:
        return

    # In case running on VPS, set timezone to correct one: 'Asia/Shanghai'
    time_zone = pytz.timezone(check_in_sheet.timezone)
    now = datetime.now(time_zone)
    # we want single digits to always be 0 or 5, etc: 9:57 -> 10:00
    adjusted_min = now.minute - now.minute % 5
    adjusted_check_in_time = now.replace(minute=adjusted_min)

    worksheet = select_worksheet(check_in_sheet, now)
    # Get User row by employee ID, and today's column
    user_row, today_col = get_cells_to_update(
        worksheet, config["employee_id"], now)
    status_cell = worksheet.cell(user_row.row, today_col.col)
    check_in_cell = worksheet.cell(user_row.row+1, today_col.col)
    check_out_cell = worksheet.cell(user_row.row+2, today_col.col)

    # Check if already checked in
    if check_in_cell.value:
        logger.info(f"Already checked in at {check_in_cell.value}")
        check_in_time = datetime.strptime(check_in_cell.value, "%H:%M")
        check_out_time = get_adjusted_checkout_time(check_in_time, now)
        # set value_input_option to RAW to USER_ENTERED to make system convert time to number
        worksheet.update(check_out_cell.address, check_out_time.strftime(
            "%H:%M"), value_input_option='USER_ENTERED')
        logger.success("Checked out at " + check_out_time.strftime("%H:%M"))
    else:
        worksheet.update(status_cell.address, "V")
        worksheet.update(check_in_cell.address, adjusted_check_in_time.strftime(
            "%H:%M"), value_input_option='USER_ENTERED')
        logger.success(f"Checked in at {adjusted_check_in_time}")


def _sanitize_time(time_str: str):
    """Sanitize time string like 18:00:00 to 18:00"""
    if sanitized_time := re.findall(r"\d+:\d+", time_str):
        return sanitized_time[0]


def _get_wokring_hour_for_sheet(worksheet: gspread.Worksheet, user_row: gspread.Cell) -> int:
    """Get working hour for the given worksheet"""
    # Save all check in and check out times into list to avoid api rate limit
    month_working_hours = 0.0
    all_check_in_times = worksheet.row_values(user_row.row+1)[2:]
    all_check_out_times = worksheet.row_values(user_row.row+2)[2:]
    for check_in_time, check_out_time in zip(all_check_in_times, all_check_out_times):
        if not check_in_time:
            continue
        try:
            check_in_time = datetime.strptime(
                _sanitize_time(check_in_time), "%H:%M")
            check_out_time = datetime.strptime(
                _sanitize_time(check_out_time), "%H:%M")
        except (ValueError, TypeError):
            continue

        hours_elapsed = (check_out_time - check_in_time).seconds / 3600
        # Check if work through noon
        if check_in_time.hour <= 12:
            hours_elapsed -= 1
        # Check if elapsed minutes is more than 30, then fit it to 0.5
        hour, minute = divmod(hours_elapsed, 1)
        minute = 0.5 if minute >= 0.5 else 0
        hours_elapsed = hour + minute
        month_working_hours += hours_elapsed
        logger.debug(
            f"{check_in_time.strftime('%H:%M')} - {check_out_time.strftime('%H:%M')} = {hours_elapsed}")
    return month_working_hours


@logger.catch
def fetch_working_hours(year: int):
    """Fetch total working hours from the google sheet for the given year"""
    config = load_config()
    if not config:
        return
    check_in_sheet = get_check_in_sheet(config["checkin_sheet_key"])
    if not check_in_sheet:
        return

    employee_id = config["employee_id"]
    worksheets = (
        w
        for w in check_in_sheet.worksheets()
        if re.match(f"{year}\d+", w.title)
    )
    # logger.debug(check_in_sheet.worksheets())
    # logger.debug(list(worksheets))

    total_working_hours = 0.0
    for worksheet in worksheets:
        logger.info(f"Fetching {worksheet.title}...")
        search_regex = re.compile(rf"{employee_id}-")
        user_row = worksheet.find(search_regex, in_column=1)
        if not user_row:
            continue
        logger.debug(f"Matched {search_regex} at row {user_row.row}")
        month_working_hours = _get_wokring_hour_for_sheet(worksheet, user_row)
        total_working_hours += month_working_hours
        logger.info(
            f"WorkSheet: {worksheet.title} | Total working hours : {month_working_hours}")

    logger.info(f"Total working hours for {year}: {total_working_hours}")


def init_parser():
    """Argparse initialization"""
    parser = argparse.ArgumentParser(
        description='GSS實習生通用腳本\nhttps://github.com/opabravo/intern-auto-checkin')
    parser.add_argument('-v', '--verbose',
                        action='store_true', help='顯示詳細資訊，並存到logs.log')
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        '-c', '--check-in', action="store_true", help='一鍵簽到')
    group.add_argument(
        '-s', '--show-working-hours', type=int, help='Fetch總工作時數 <年分:int>，例如：-s 2022')
    return parser


if __name__ == "__main__":
    try:
        config_file = Path("service_acc.json")
        gc = gspread.service_account(filename=config_file)
    except FileNotFoundError:
        logger.error("Service account file not found")
    except ValueError:
        logger.error("Service account config is not valid")
    except Exception as e:
        logger.exception(e)
    else:
        parser = init_parser()
        args = parser.parse_args()
        # Init Logger
        if args.verbose:
            logger.add("logs.log", rotation="5 MB",
                       backtrace=True, diagnose=True)
        else:
            logger.remove(0)
            logger.add(
                sys.stderr, format="<level>{message}</level>", level="INFO", colorize=True)

        if year := args.show_working_hours:
            fetch_working_hours(year)
        elif args.check_in:
            main()
        else:
            parser.print_help()
    input("\nPress any key to exit...")

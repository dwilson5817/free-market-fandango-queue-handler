import datetime
import os
import random
from decimal import Decimal
from uuid import uuid4

import boto3
from boto3.dynamodb.conditions import Key

from constants import Settings, StockPriceChangeReason
from utils import update_current_event_rotate_time_if_needed, update_price_rotate_time_if_needed, change_stock_price

KEY_SEPERATOR = '#'

table_name = os.environ["DYNAMODB_TABLE_ARN"]
table = boto3.resource("dynamodb").Table(table_name)


def build_key(*args):
    return KEY_SEPERATOR.join(args)


def get_settings():
    response = table.query(
        KeyConditionExpression=Key("PK").eq("Setting")
    )["Items"]

    return { setting: int(next(item for item in response if item["SK"] == setting.value)["Value"]) for setting in Settings }


def get_lowest_allowed_price(initial_price: Decimal, max_percent_loss: int):
    return initial_price - (initial_price * Decimal(100 / max_percent_loss))


def handler(event, context):
    for record in event['Records']:
        print("Received:", record)
        event_type = record["body"]
        message_attributes = record["messageAttributes"]

        match event_type:
            case "RotateEvent":
                return handle_rotate_event(message_attributes)
            case "RotatePrice":
                return handle_rotate_price(message_attributes)
            case "Purchase":
                return handle_purchase(message_attributes)
            case "CacheInvalid":
                return handle_cache_invalid(message_attributes)
            case _:
                print("FATAL: Event", event_type, "is not valid!")
                raise Exception("Invalid event type")


def handle_rotate_event(message_attributes):
    active_market_uuid = message_attributes["MarketUUID"]["stringValue"]

    all_events = table.query(
        KeyConditionExpression=(
                Key("PK").eq(f"Event")
        )
    )["Items"]

    events_run = table.query(
        KeyConditionExpression=(
                Key("PK").eq(f"Market#{active_market_uuid}") &
                Key("SK").begins_with("Event")
        )
    )["Items"]

    # Hello future me!  Good luck with the following lines, you are gonna need it...
    event_total_runs = [{'UUID': event["SK"], 'TotalRuns': sum(value["UUID"] == event["SK"] for value in events_run)} for event in all_events]
    print('\n'.join([f"{event['UUID']} has run {event['TotalRuns']} times" for event in event_total_runs ]))

    min_count = min(event_total_runs, key=lambda x: x['TotalRuns'])['TotalRuns']
    print(f"Looking for events which have run {min_count} times")

    potential_next_events = [event for event in event_total_runs if event['TotalRuns'] == min_count]
    print(f"Considering the following events: {', '.join([potential_event['UUID'] for potential_event in potential_next_events])}")

    next_event_uuid = random.choice(potential_next_events)["UUID"]
    print(f"Picked event {next_event_uuid}")

    next_event = next(event for event in all_events if event["SK"] == next_event_uuid)
    print(f"Event title: '{next_event['Title']}'")

    settings = get_settings()
    event_min_duration = settings[Settings.NEWS_MIN_DURATION]
    event_max_duration = settings[Settings.NEWS_MAX_DURATION]

    next_event_length = int(random.uniform(event_min_duration, event_max_duration))
    event_started_at = datetime.datetime.now()
    next_event_change = event_started_at + datetime.timedelta(minutes=next_event_length)

    table.update_item(
        Key={
            "PK": f'Market',
            "SK": f'Active',
        },
        ExpressionAttributeNames={
            '#CurrentEvent': 'CurrentEvent',
            '#CurrentEventRotate': 'CurrentEventRotate',
        },
        ExpressionAttributeValues={
            ':CurrentEvent': next_event_uuid,
            ':CurrentEventRotate': next_event_change.isoformat(),
        },
        UpdateExpression='SET #CurrentEvent = :CurrentEvent, #CurrentEventRotate = :CurrentEventRotate',
    )

    table.update_item(
        Key={
            "PK": f'Market#{active_market_uuid}',
            "SK": f'Details',
        },
        ExpressionAttributeNames={
            '#CurrentEvent': 'CurrentEvent',
        },
        ExpressionAttributeValues={
            ':CurrentEvent': next_event_uuid,
        },
        UpdateExpression='SET #CurrentEvent = :CurrentEvent',
    )

    table.put_item(
        Item={
            'PK': f"Market#{active_market_uuid}",
            'SK': f"Event#{next_event_uuid}#{datetime.datetime.now().isoformat()}",
            "StartedAt": event_started_at.isoformat(),
            "UUID": next_event_uuid,
        }
    )

    all_stocks = table.query(
        KeyConditionExpression=Key("PK").eq("Stock")
    )["Items"]

    already_changed = []

    print("Checking for stocks which are affected by this event rotation")

    for tag in next_event["Tags"]:
        print(f"Checking {tag}")

        for stock in all_stocks:
            stock_code = stock["SK"]

            if tag in stock["Tags"] and stock_code not in already_changed:
                print(f"{stock_code} is about to have it's price changed")

                stock_price_record = table.get_item(
                    Key={
                        "PK": f'Market#{active_market_uuid}',
                        "SK": f'Stock#{stock_code}',
                    }
                )["Item"]

                change_stock_price(
                    table,
                    active_market_uuid,
                    stock_price_record["Version"],
                    stock_code,
                    stock_price_record["Price"],
                    int(next_event["ChangeMin"]),
                    int(next_event["ChangeMax"]),
                    settings[Settings.STOCK_NO_PURCHASE_LOSS_TIME],
                    StockPriceChangeReason.EVENT,
                    min_stock_price=get_lowest_allowed_price(stock["InitialPrice"], settings[Settings.STOCK_MAX_PERCENT_LOSS]),
                    price_rotate_time=event_started_at.isoformat()
                )

                already_changed.append(stock_code)

    print(f"The following stocks were affected: {', '.join(already_changed)}")


def handle_rotate_price(message_attributes: dict):
    active_market_uuid = message_attributes["MarketUUID"]["stringValue"]
    stock_code = message_attributes["StockCode"]["stringValue"]

    print("Handling price rotation")

    stock_cache_result = table.get_item(
        Key={
            "PK": f'Market#{active_market_uuid}',
            "SK": f'Stock#{stock_code}',
        }
    )

    stock_result = table.get_item(
        Key={
            "PK": 'Stock',
            "SK": stock_code,
        }
    )

    if "Item" not in stock_cache_result:
        raise Exception("Stock price is not yet cached")

    if "Item" not in stock_result:
        raise Exception("Attempting to rotate price for stock which doesn't exist")

    stock_price_record = stock_cache_result["Item"]
    stock_record = stock_result["Item"]

    stock_record_version: str = stock_price_record["Version"]
    old_stock_price: Decimal = stock_price_record["Price"]

    settings = get_settings()

    min_stock_increase = -settings[Settings.STOCK_NO_PURCHASE_MIN_LOSS]
    max_stock_increase = -settings[Settings.STOCK_NO_PURCHASE_MAX_LOSS]
    no_purchase_loss_time = settings[Settings.STOCK_NO_PURCHASE_LOSS_TIME]

    lowest_allowed_price = get_lowest_allowed_price(stock_record["InitialPrice"], settings[Settings.STOCK_MAX_PERCENT_LOSS])

    change_stock_price(
        table,
        active_market_uuid,
        stock_record_version,
        stock_code,
        old_stock_price,
        min_stock_increase,
        max_stock_increase,
        no_purchase_loss_time,
        StockPriceChangeReason.NO_PURCHASE,
        min_stock_price=lowest_allowed_price
    )


def handle_purchase(message_attributes: dict):
    print("Handling purchase")

    active_market_uuid = message_attributes["MarketUUID"]["stringValue"]
    stock_code = message_attributes["StockCode"]["stringValue"]

    stock_price_record = table.get_item(
        Key={
            "PK": f'Market#{active_market_uuid}',
            "SK": f'Stock#{stock_code}',
        }
    )

    if "Item" not in stock_price_record:
        raise Exception("Stock price is not yet cached")

    old_stock_price = stock_price_record["Item"]["Price"]
    stock_record_version = stock_price_record["Item"]["Version"]

    settings = get_settings()

    min_stock_increase = settings[Settings.STOCK_PURCHASE_MIN_INCREASE]
    max_stock_increase = settings[Settings.STOCK_PURCHASE_MAX_INCREASE]
    no_purchase_loss_time = settings[Settings.STOCK_NO_PURCHASE_LOSS_TIME]

    change_stock_price(
        table,
        active_market_uuid,
        stock_record_version,
        stock_code,
        old_stock_price,
        min_stock_increase,
        max_stock_increase,
        no_purchase_loss_time,
        StockPriceChangeReason.PURCHASE,
    )


def find_changes(items, cached_items, find_cached_key):
    item_ids = {item["SK"] for item in items}
    cached_item_ids = {find_cached_key(cached_item) for cached_item in cached_items}

    return item_ids - cached_item_ids, item_ids & cached_item_ids, cached_item_ids - item_ids


def handle_cache_invalid(message_attributes: dict):
    print("Handling invalid cache")

    active_market_uuid = message_attributes["MarketUUID"]["stringValue"]

    all_stocks = table.query(
        KeyConditionExpression=Key("PK").eq("Stock")
    )["Items"]

    stocks_cache = table.query(
        KeyConditionExpression=(
                Key("PK").eq(build_key("Market", active_market_uuid)) &
                Key("SK").begins_with("Stock")
        ),
    )["Items"]

    stocks_created, stocks_not_changed, stocks_deleted = find_changes(
        all_stocks,
        stocks_cache,
        lambda item: item["SK"].split(KEY_SEPERATOR)[1]
    )

    all_cards = table.query(
        KeyConditionExpression=Key("PK").eq("Card")
    )["Items"]

    cards_created, _, cards_deleted = find_changes(
        all_cards,
        table.query(
            KeyConditionExpression=(
                    Key("PK").eq(build_key("Market", active_market_uuid)) &
                    Key("SK").begins_with("Card")
            ),
        )["Items"],
        lambda item: item["SK"].split(KEY_SEPERATOR)[1]
    )

    settings = get_settings()
    no_purchase_loss_time = settings[Settings.STOCK_NO_PURCHASE_LOSS_TIME]

    new_market = table.get_item(
        Key={
            'PK': build_key("Market", active_market_uuid),
            'SK': "Details",
        }
    )["Item"]

    new_market_opened_at = datetime.datetime.fromisoformat(new_market["OpenedAt"])

    with table.batch_writer() as batch:
        # If a stock was deleted, we remove the cached value (price history is kept, and can still be queried)

        for stock_code in stocks_deleted:
            batch.delete_item(
                Key={
                    'PK': build_key('Market', active_market_uuid),
                    'SK': build_key('Stock', stock_code)
                },
            )

        # Same for cards...

        for card_number in cards_deleted:
            batch.delete_item(
                Key={
                    'PK': build_key('Market', active_market_uuid),
                    'SK': build_key('Card', card_number)
                },
            )

        # If a stock was just created (or the market was just opened), we need to cache it

        for stock_code in stocks_created:
            stock = next(stock for stock in all_stocks if stock["SK"] == stock_code)

            batch.put_item(
                Item={
                    'PK': build_key("Market", active_market_uuid),
                    'SK': build_key("Stock", stock_code),
                    'Price': stock["InitialPrice"],
                    'Version': str(uuid4()),
                    'PriceRotate': (new_market_opened_at + datetime.timedelta(minutes=no_purchase_loss_time)).isoformat()
                }
            )

        # Same for cards...

        for card_number in cards_created:
            card = next(card for card in all_cards if card["SK"] == card_number)

            batch.put_item(
                Item={
                    'PK': build_key("Market", active_market_uuid),
                    'SK': build_key("Card", card["SK"]),
                    'Balance': card["Balance"],
                }
            )

        # Below the cards exist, and they are cached, but the value for STOCK_NO_PURCHASE_LOSS_TIME might have changed,
        # so we need to we calculate it again

        for stock_code in stocks_not_changed:
            update_price_rotate_time_if_needed(
                table,
                active_market_uuid,
                new_market,
                stock_code,
                next(cached_stock for cached_stock in stocks_cache if cached_stock["SK"].split(KEY_SEPERATOR)[1] == stock_code),
                settings
            )

        active_market_record = table.get_item(
            Key={
                'PK': "Market",
                'SK': "Active",
            }
        )

        current_event_rotate = None

        if "Item" in active_market_record and "UUID" in active_market_record["Item"] and active_market_uuid == active_market_record["Item"]["UUID"] and active_market_record["Item"]["CurrentEvent"] is not None:
            current_event_rotate = update_current_event_rotate_time_if_needed(
                table,
                active_market_uuid,
                active_market_record["Item"],
                settings
            )

        batch.put_item(
            Item={
                'PK': "Market",
                'SK': "Active",
                'UUID': active_market_uuid,
                'OpenedAt': new_market["OpenedAt"],
                'ClosedAt': new_market["ClosedAt"],
                'CurrentEvent': new_market["CurrentEvent"],

                # Used by queue handler to quickly figure out when to rotate events and stock prices
                'CurrentEventRotate': current_event_rotate,
            }
        )

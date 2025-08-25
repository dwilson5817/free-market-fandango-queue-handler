import random
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import uuid4

from boto3.dynamodb.conditions import Key, Attr

from .constants import Settings, StockPriceChangeReason

KEY_SEPERATOR = '#'


def build_key(*args):
    return KEY_SEPERATOR.join(args)


def change_stock_price(
        table,
        active_market_uuid: str,
        stock_record_version: str,
        stock_code: str,
        old_stock_price: Decimal,
        min_stock_increase: int,
        max_stock_increase: int,
        no_purchase_loss_time: int,
        reason: StockPriceChangeReason,
        price_rotate_time: str | None = None,
        min_stock_price: Decimal = None,
):
    change_pct = Decimal(random.uniform(min_stock_increase, max_stock_increase))
    new_stock_price = old_stock_price + round(old_stock_price * change_pct / 100, 2)

    if min_stock_price is not None and new_stock_price < min_stock_price:
        print("New stock price would be too low, will not reduce")
        return

    print("New stock price will be", new_stock_price)

    price_change_time = datetime.now()

    # Attempt to update, error thrown if the price changed by the time we tried to update, in this case the Lambda will
    # fail and the event will be run again shortly.
    if price_rotate_time is not None:
        next_stock_price_change = price_change_time + timedelta(minutes=no_purchase_loss_time)

        table.update_item(
            Key={
                "PK": f'Market#{active_market_uuid}',
                "SK": f'Stock#{stock_code}',
            },
            ExpressionAttributeNames={
                '#Price': 'Price',
                '#PriceRotate': 'PriceRotate',
                '#Version': 'Version',
            },
            ExpressionAttributeValues={
                ':Price': Decimal(new_stock_price),
                ':PriceRotate': next_stock_price_change.isoformat(),
                ':Version': str(uuid4()),
            },
            UpdateExpression='SET #Price = :Price, #PriceRotate = :PriceRotate, #Version = :Version',
            ConditionExpression=Attr('Version').eq(stock_record_version),
        )
    else:
        table.update_item(
            Key={
                "PK": f'Market#{active_market_uuid}',
                "SK": f'Stock#{stock_code}',
            },
            ExpressionAttributeNames={
                '#Price': 'Price',
                '#Version': 'Version',
            },
            ExpressionAttributeValues={
                ':Price': Decimal(new_stock_price),
                ':Version': str(uuid4()),
            },
            UpdateExpression='SET #Price = :Price, #Version = :Version',
            ConditionExpression=Attr('Version').eq(stock_record_version),
        )

    # If we were able to update it, we can safely put the price change in DynamoDB too.
    table.put_item(
        Item={
            'PK': f"Market#{active_market_uuid}",
            'SK': f"Price#{stock_code}#{price_change_time.isoformat()}",
            "PreviousPrice": Decimal(str(old_stock_price)),
            "Reason": reason.value,
        }
    )


def get_last_price_change(table, market_uuid: str, stock_code: str | None = None) -> dict[str, Any] | None:
    result = table.query(
        KeyConditionExpression=(
                Key("PK").eq(build_key("Market", market_uuid)) &
                Key("SK").begins_with("Price" if stock_code is None else build_key("Price", stock_code))
        ),
        Limit=1,
        ScanIndexForward=False,
    )["Items"]

    return result[0] if len(result) > 0 else None


def update_price_rotate_time_if_needed(
        table,
        market_uuid: str,
        market_record: dict,
        stock_code: str,
        cached_stock_record: dict,
        all_settings: dict[Settings, int],
):
    no_purchase_loss_time = all_settings[Settings.STOCK_NO_PURCHASE_LOSS_TIME]

    # Find the last price change, setting ScanIndexForward starts at the HIGHEST sort key (i.e. the latest
    # because they are identical except the date and time

    last_stock_price_change = get_last_price_change(table, market_uuid, stock_code)

    # A bit messy but price_changes is an array containing 1 item: the last price change.  Split the sort key
    # for this item by the seperator, the 3rd item is the time of the change ("Price" literal, stock code, time)
    last_stock_price_change_time = last_stock_price_change["SK"].split(KEY_SEPERATOR)[2] if last_stock_price_change is not None else market_record["OpenedAt"]

    current_rotate_at = datetime.fromisoformat(cached_stock_record["PriceRotate"])
    current_event_duration = current_rotate_at - datetime.fromisoformat(last_stock_price_change_time)

    if current_event_duration == timedelta(minutes=no_purchase_loss_time):
        return

    next_stock_price_change = datetime.fromisoformat(last_stock_price_change_time) + timedelta(minutes=no_purchase_loss_time)

    table.update_item(
        Key={
            "PK": f'Market#{market_uuid}',
            "SK": f'Stock#{stock_code}',
        },
        ExpressionAttributeNames={
            '#PriceRotate': 'PriceRotate',
            '#Version': 'Version',
        },
        ExpressionAttributeValues={
            ':PriceRotate': next_stock_price_change.isoformat(),
            ':Version': str(uuid4()),
        },
        UpdateExpression='SET #PriceRotate = :PriceRotate, #Version = :Version',
        ConditionExpression=Attr('Version').eq(cached_stock_record["Version"]),
    )


def update_current_event_rotate_time_if_needed(
        table,
        market_uuid: str,
        market_record: dict,
        all_settings: dict[Settings, int]
):
    news_min_duration = all_settings[Settings.NEWS_MIN_DURATION]
    news_max_duration = all_settings[Settings.NEWS_MAX_DURATION]

    current_event = table.query(
        KeyConditionExpression=(
                Key("PK").eq(f"Market#{market_uuid}") &
                Key("SK").begins_with(build_key("Event", market_record["CurrentEvent"]))
        ),
        Limit=1,
        ScanIndexForward=False,
    )["Items"][0]

    current_event_rotate = datetime.fromisoformat(market_record["CurrentEventRotate"])
    current_event_started_at = datetime.fromisoformat(current_event["StartedAt"])

    current_event_duration = current_event_rotate - current_event_started_at

    if timedelta(minutes=news_min_duration) <= current_event_duration <= timedelta(minutes=news_max_duration):
        return current_event_rotate.isoformat()

    current_event_new_duration = int(random.uniform(news_min_duration, news_max_duration))
    current_event_new_rotate = current_event_started_at + timedelta(minutes=current_event_new_duration)

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
            ':CurrentEvent': current_event["UUID"],
            ':CurrentEventRotate': current_event_new_rotate.isoformat(),
        },
        UpdateExpression='SET #CurrentEvent = :CurrentEvent, #CurrentEventRotate = :CurrentEventRotate',
    )

    return current_event_new_rotate.isoformat()

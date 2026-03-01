"""
generators/products.py

Generates realistic product records for the e-commerce platform.

Fields: product_id, product_name, category, cost_price, selling_price, inventory_quantity
"""

import csv
import io
import uuid
from random import choices, randint, uniform

CATEGORIES = {
    "Electronics":   0.25,
    "Fashion":       0.20,
    "Home & Garden": 0.15,
    "Sports":        0.12,
    "Beauty":        0.10,
    "Books":         0.08,
    "Toys":          0.06,
    "Automotive":    0.04,
}

PRODUCT_NAMES = {
    "Electronics":   ["Wireless Headphones", "Smart Watch", "USB-C Hub", "Bluetooth Speaker", "Laptop Stand"],
    "Fashion":       ["Running Sneakers", "Leather Wallet", "Denim Jacket", "Summer Dress", "Wool Scarf"],
    "Home & Garden": ["Air Purifier", "LED Desk Lamp", "Bamboo Cutting Board", "Herb Planter", "Smart Thermostat"],
    "Sports":        ["Yoga Mat", "Resistance Bands", "Foam Roller", "Dumbbell Set", "Cycling Gloves"],
    "Beauty":        ["Vitamin C Serum", "Moisturising Cream", "Lip Palette", "Face Mask Kit", "Hair Growth Oil"],
    "Books":         ["Data Engineering Fundamentals", "The Pragmatic Programmer", "Clean Code", "Python Cookbook", "SQL Mastery"],
    "Toys":          ["Building Blocks Set", "RC Car", "Puzzle (1000 pc)", "Plush Dinosaur", "Art & Craft Kit"],
    "Automotive":    ["Car Phone Mount", "Tyre Inflator", "Seat Organiser", "Dashboard Cam", "Jump Starter"],
}

CATEGORY_PRICE_RANGES = {
    "Electronics":   (20.0, 500.0),
    "Fashion":       (10.0, 200.0),
    "Home & Garden": (15.0, 300.0),
    "Sports":        (8.0,  150.0),
    "Beauty":        (5.0,  80.0),
    "Books":         (5.0,  40.0),
    "Toys":          (5.0,  100.0),
    "Automotive":    (10.0, 250.0),
}

PROFIT_MARGIN_RANGE = (0.20, 0.60)  # selling price is 20-60% above cost


def generate_products(batch_size: int) -> str:
    """
    Generate a batch of product records and return them as a CSV string.

    Parameters
    ----------
    batch_size : Number of product records to generate.

    Returns
    -------
    CSV-formatted string with header row and `batch_size` data rows.
    """
    categories = list(CATEGORIES.keys())
    weights = list(CATEGORIES.values())

    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(
        ["product_id", "product_name", "category", "cost_price", "selling_price", "inventory_quantity"]
    )

    for _ in range(batch_size):
        category = choices(categories, weights=weights)[0]
        name = choices(PRODUCT_NAMES[category])[0]
        low, high = CATEGORY_PRICE_RANGES[category]
        cost_price = round(uniform(low, high), 2)
        margin = uniform(*PROFIT_MARGIN_RANGE)
        selling_price = round(cost_price * (1 + margin), 2)
        inventory_quantity = randint(0, 1000)

        writer.writerow([
            str(uuid.uuid4()),
            name,
            category,
            cost_price,
            selling_price,
            inventory_quantity,
        ])

    return output.getvalue()

test_texts = [
    "BTC/USDT vor 4 Stunden Abgebrochen Kaufen bei 106050",
    "ETH/USDT vor 2 Stunden Ausgeführt Gekauft bei 2500", 
    "XRP/USDT vor 1 Stunde Terminiert",
    "ADA/USDT vor 30 Min Kaufen bei 0.8151"
]

termination_keywords = [
    'terminiert', 'terminated', 'ausgeführt', 'executed', 
    'filled', 'abgelaufen', 'expired', 'geschlossen', 'closed',
    'abgebrochen', 'cancelled', 'canceled'
]

for text in test_texts:
    is_terminated = any(keyword in text.lower() for keyword in termination_keywords)
    print(f"'{text}' -> {'TERMINATE' if is_terminated else 'ACTIVE'}")
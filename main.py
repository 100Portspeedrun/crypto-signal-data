import time
import random
import re
import os
import json
import hashlib
import logging
from datetime import datetime
from typing import List, Optional, Tuple

# Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class Signal:
    """Signal Datenstruktur"""
    def __init__(self, symbol: str, direction: str, entry_price: float, 
                 take_profit: float, stop_loss: float, timestamp: str):
        self.symbol = symbol
        self.direction = direction  # 'LONG', 'SHORT' oder 'TERMINATE'
        self.entry_price = entry_price
        self.take_profit = take_profit
        self.stop_loss = stop_loss
        self.timestamp = timestamp
        self.signal_id = self._generate_id()
    
    def _generate_id(self) -> str:
        """Eindeutige Signal ID generieren"""
        data = f"{self.symbol}_{self.direction}_{self.entry_price}_{self.timestamp[:16]}"
        return hashlib.md5(data.encode()).hexdigest()[:12]
    
    def to_dict(self) -> dict:
        """Signal als Dictionary"""
        return {
            'signal_id': self.signal_id,
            'symbol': self.symbol,
            'direction': self.direction,
            'entry_price': self.entry_price,
            'take_profit': self.take_profit,
            'stop_loss': self.stop_loss,
            'timestamp': self.timestamp,
            'processed': False
        }
    
    def __str__(self):
        if self.direction == 'TERMINATE':
            return f"{self.symbol} TERMINATED"
        return f"{self.symbol} {self.direction} @ {self.entry_price} (TP: {self.take_profit}, SL: {self.stop_loss})"

class SignalStorage:
    """JSON-basierte Signal-Speicherung mit Termination-Support"""
    
    def __init__(self, base_path: str = "."):
        self.signals_dir = os.path.join(base_path, "signals")
        self.current_file = os.path.join(self.signals_dir, "current.json")
        self.processed_file = os.path.join(self.signals_dir, "processed.json")
        self.status_file = os.path.join(self.signals_dir, "status.json")
        
        os.makedirs(self.signals_dir, exist_ok=True)
        self._init_files()
    
    def _init_files(self):
        """Initialisiere JSON-Dateien"""
        if not os.path.exists(self.current_file):
            self._write_json(self.current_file, {
                "signals": [], 
                "terminated_signals": [],
                "last_update": ""
            })
        
        if not os.path.exists(self.processed_file):
            self._write_json(self.processed_file, {"processed_ids": []})
            
        if not os.path.exists(self.status_file):
            self._write_json(self.status_file, {
                "last_scrape": "",
                "active_signals": 0,
                "terminated_signals": 0
            })
    
    def _read_json(self, filepath: str) -> dict:
        """JSON-Datei lesen"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}
    
    def _write_json(self, filepath: str, data: dict):
        """JSON-Datei schreiben"""
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    
    def get_processed_ids(self) -> List[str]:
        """Liste der bereits verarbeiteten Signal-IDs"""
        data = self._read_json(self.processed_file)
        return data.get('processed_ids', [])
    
    def add_processed_ids(self, signal_ids: List[str]):
        """Signal-IDs als verarbeitet markieren"""
        current_ids = self.get_processed_ids()
        new_ids = list(set(current_ids + signal_ids))
        
        # Limitiere auf letzte 1000 IDs
        if len(new_ids) > 1000:
            new_ids = new_ids[-1000:]
        
        self._write_json(self.processed_file, {"processed_ids": new_ids})
    
    def save_signals(self, signals: List[Signal]) -> Tuple[List[Signal], List[Signal]]:
        """Aktuelle Signale speichern und nach Status gruppieren"""
        # Signale nach Status gruppieren
        active_signals = [s for s in signals if s.direction != 'TERMINATE']
        terminated_signals = [s for s in signals if s.direction == 'TERMINATE']
        
        data = {
            "signals": [signal.to_dict() for signal in active_signals],
            "terminated_signals": [signal.to_dict() for signal in terminated_signals],
            "last_update": datetime.now().isoformat(),
            "total_signals": len(active_signals),
            "terminated_count": len(terminated_signals)
        }
        self._write_json(self.current_file, data)
        
        # Status-Summary für Trading Bot
        status_data = {
            "last_scrape": datetime.now().isoformat(),
            "active_signals": len(active_signals),
            "terminated_signals": len(terminated_signals),
            "symbols_active": [s.symbol for s in active_signals],
            "symbols_terminated": [s.symbol for s in terminated_signals]
        }
        self._write_json(self.status_file, status_data)
        
        logger.info(f"Saved {len(active_signals)} active signals, {len(terminated_signals)} terminated")
        
        # Log terminierte Signale besonders prominent
        for signal in terminated_signals:
            logger.critical(f"TERMINATED: {signal.symbol} - Trading Bot should close position!")
        
        return active_signals, terminated_signals

class CryptetScraper:
    """Cryptet.com Signal Scraper mit Termination Detection"""
    
    def __init__(self, url: str = "https://cryptet.com/de/"):
        self.url = url
        self.driver = None
        self.storage = SignalStorage()
        
        # Nur diese Symbole werden verarbeitet
        self.target_symbols = [
            'BTCUSDT', 'ETHUSDT', 'XRPUSDT', 'ADAUSDT', 'SOLUSDT',
            'DOGEUSDT', 'LTCUSDT', 'TRXUSDT', 'LINKUSDT'
        ]
    
    def setup_driver(self):
        """Chrome Driver einrichten"""
        options = Options()
        options.add_argument('--headless')  # Ohne sichtbares Fenster
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        
        try:
            self.driver = webdriver.Chrome(options=options)
            logger.info("Chrome Driver initialized successfully")
            return True
        except Exception as e:
            logger.error(f"Chrome Driver failed: {e}")
            return False
    
    def start(self):
        """Hauptloop - kontinuierliches Scraping"""
        logger.info("STARTING Cryptet Signal Scraper v3 - WITH TERMINATION DETECTION")
        logger.info(f"Target URL: {self.url}")
        logger.info(f"Target Symbols: {', '.join(self.target_symbols)}")
        
        if not self.setup_driver():
            logger.error("Driver setup failed")
            return
        
        try:
            while True:
                try:
                    self.scrape_cycle()
                    
                    # Zufälliges Intervall (18-22 Minuten)
                    interval = random.uniform(18*60, 22*60)
                    logger.info(f"Next scrape in {interval/60:.1f} minutes")
                    time.sleep(interval)
                    
                except KeyboardInterrupt:
                    logger.info("Scraper stopped by user")
                    break
                except Exception as e:
                    logger.error(f"Scraping error: {e}")
                    time.sleep(300)  # 5 Min warten bei Fehler
        finally:
            if self.driver:
                self.driver.quit()
                logger.info("Driver closed")
    
    def scrape_cycle(self):
        """Ein Scraping-Zyklus"""
        logger.info("Starting scrape cycle...")
        
        try:
            # Website laden
            self.driver.get(self.url)
            time.sleep(random.uniform(3, 7))
            
            # Signale extrahieren
            signals = self.extract_signals()
            logger.info(f"Found {len(signals)} total signals")
            
            # Nur Target-Symbols filtern
            filtered_signals = [s for s in signals if s.symbol in self.target_symbols]
            logger.info(f"Filtered to {len(filtered_signals)} relevant signals")
            
            # Nur neue Signale behalten
            new_signals = self.filter_new_signals(filtered_signals)
            
            if new_signals:
                logger.info(f"NEW SIGNALS: {len(new_signals)} found!")
                
                # Signale anzeigen
                for signal in new_signals:
                    logger.info(f"  {signal}")
                
                # Speichern und nach Status gruppieren
                active_signals, terminated_signals = self.storage.save_signals(new_signals)
                
                # Als verarbeitet markieren
                new_ids = [signal.signal_id for signal in new_signals]
                self.storage.add_processed_ids(new_ids)
                
                # Spezielle Behandlung für terminierte Signale
                if terminated_signals:
                    logger.warning(f"EMERGENCY: {len(terminated_signals)} positions should be closed immediately!")
                    for term_signal in terminated_signals:
                        logger.critical(f"CLOSE NOW: {term_signal.symbol}")
                
            else:
                logger.info("No new signals found")
                
        except Exception as e:
            logger.error(f"Scrape cycle failed: {e}")
    
    def extract_signals(self) -> List[Signal]:
        """Signale von der Website extrahieren"""
        signals = []
        
        try:
            signal_cards = self.driver.find_elements(By.CSS_SELECTOR, ".signal-card")
            logger.debug(f"Found {len(signal_cards)} signal cards")
            
            for i, card in enumerate(signal_cards):
                try:
                    signal = self.parse_signal_card(card)
                    if signal:
                        signals.append(signal)
                except Exception as e:
                    logger.debug(f"Card {i+1} parsing failed: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"Signal extraction failed: {e}")
        
        return signals
    
    def parse_signal_card(self, card) -> Optional[Signal]:
        """Parse einzelne Signal-Card mit Termination Detection"""
        try:
            # Symbol extrahieren
            symbol_elem = card.find_element(By.CSS_SELECTOR, "a[href*='signals/last']")
            symbol_text = symbol_elem.text.strip()
            
            if '/' not in symbol_text or 'USDT' not in symbol_text:
                return None
            
            symbol = symbol_text.replace('/', '').upper()  # BTC/USDT → BTCUSDT
            
            # Check für terminierte/abgelaufene Signale
            card_text = card.text.lower()
            card_classes = card.get_attribute('class').lower()
            
            # Status-Keywords die sofortigen Position-Close erfordern
            termination_keywords = [
                'terminiert', 'terminated', 'ausgeführt', 'executed', 
                'filled', 'abgelaufen', 'expired', 'geschlossen', 'closed',
                'abgebrochen', 'cancelled', 'canceled'
            ]
            
            # Prüfe auf Termination
            is_terminated = any(keyword in card_text for keyword in termination_keywords)
            is_filled = 'filled' in card_classes
            
            if is_terminated or is_filled:
                # Erstelle Termination-Signal für Trading Bot
                logger.info(f"TERMINATED SIGNAL DETECTED: {symbol}")
                timestamp = datetime.now().isoformat()
                return Signal(
                    symbol=symbol,
                    direction='TERMINATE',  # Spezieller Status
                    entry_price=0.0,
                    take_profit=0.0,
                    stop_loss=0.0,
                    timestamp=timestamp
                )
            
            # Direction basierend auf CSS-Klassen
            if 'buy' in card_classes:
                direction = 'LONG'
            elif 'sell' in card_classes:
                direction = 'SHORT'
            else:
                # Fallback: Text-basierte Erkennung
                if 'kaufen' in card_text and 'verkaufen' not in card_text:
                    direction = 'LONG'
                elif 'verkaufen' in card_text and 'kaufen' not in card_text:
                    direction = 'SHORT'
                else:
                    logger.debug(f"Could not determine direction for {symbol}")
                    return None
            
            # Preise extrahieren (nur für normale Signale, nicht für TERMINATE)
            timestamp = datetime.now().isoformat()
            
            if direction == 'TERMINATE':
                return Signal(symbol, direction, 0.0, 0.0, 0.0, timestamp)
            
            price_elements = card.find_elements(By.CSS_SELECTOR, ".signal-value")
            
            if len(price_elements) < 6:
                logger.debug(f"Not enough price elements: {len(price_elements)}")
                return None
            
            # Die Preise sind bei Index 3, 4, 5 (Entry, TP, SL)
            prices = []
            for idx in [3, 4, 5]:
                if idx < len(price_elements):
                    text = price_elements[idx].text.strip()
                    # Nur Zahlen extrahieren
                    clean_price = re.sub(r'[^\d.,]', '', text).replace(',', '')
                    
                    if clean_price and clean_price.replace('.', '').isdigit():
                        prices.append(float(clean_price))
                    else:
                        return None
                else:
                    return None
            
            if len(prices) != 3:
                return None
            
            entry_price, take_profit, stop_loss = prices
            
            # Plausibilitätscheck
            if entry_price <= 0 or take_profit <= 0 or stop_loss <= 0:
                return None
            
            # Symbol-spezifische Checks
            if symbol == 'BTCUSDT' and entry_price < 1000:
                return None
            if symbol == 'ETHUSDT' and entry_price < 100:
                return None
            
            return Signal(symbol, direction, entry_price, take_profit, stop_loss, timestamp)
            
        except (NoSuchElementException, ValueError, IndexError) as e:
            logger.debug(f"Card parsing failed: {e}")
            return None
    
    def filter_new_signals(self, signals: List[Signal]) -> List[Signal]:
        """Filtere nur neue Signale"""
        processed_ids = self.storage.get_processed_ids()
        new_signals = []
        
        for signal in signals:
            if signal.signal_id not in processed_ids:
                new_signals.append(signal)
        
        return new_signals

def main():
    """Hauptfunktion"""
    scraper = CryptetScraper("https://cryptet.com/de/")
    scraper.start()

if __name__ == "__main__":
    main()
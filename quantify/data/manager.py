import pandas as pd
class DataManager:
    def __init__(self, source, store):
        self.source = source
        self.store = store
    
    def get(self, symbol, timeframe, start=None, end=None):
        return self.store.read(symbol, timeframe, start=start, end=end)

    def get_many(self, symbols, timeframe, start=None, end=None):
        return self.store.read_many(symbols, timeframe, start=start, end=end)
    
    def sync(self, symbols, timeframe, start, end):
        available_symbols = self.store.available_symbols(timeframe)
        for sym in symbols:
            if sym not in available_symbols:
                df = self.source.fetch(sym, timeframe, start, end)
                self.store.write(sym, timeframe, df)
            else:
                missing = self.store.missing_ranges(sym, timeframe, start, end)
                for s,e in missing:
                    if s == e:
                        continue
                    try:
                        df = self.source.fetch(sym, timeframe, s, e)
                        self.store.append(sym, timeframe, df)
                    except ValueError:
                        print(f"Warning: no data for {sym} {s} -> {e}, skipping...")

    def update(self, symbols,timeframe):
        end = pd.Timestamp.now(tz="UTC").date()
        available_symbols = self.store.available_symbols(timeframe)
        for sym in symbols:
            if sym not in available_symbols:
                raise ValueError(f"{sym} not in store, call sync() first")
            else:
                last_date = self.store.date_range(sym, timeframe)[-1]
                df = self.source.fetch(sym, timeframe, start=last_date, end=end)
                self.store.append(sym, timeframe, df)
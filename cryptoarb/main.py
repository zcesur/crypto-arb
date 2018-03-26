from multiprocessing import Process
from trade import arbitrage


def main():
    procs = [
        Process(target=arbitrage, args=(ex, )) for ex in ['Bittrex', 'Kraken']
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join()


if __name__ == '__main__':
    main()

"""SahlPay: generateur batch de secours (non fourni par le professeur).
Usage: python producer.py --mode file --n 50000
"""
import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

CITIES = {
    'Rabat': (34.0209, -6.8416), 'Casablanca': (33.5731, -7.5898),
    'Tanger': (35.7595, -5.8340), 'Fes': (34.0331, -5.0003),
    'Marrakech': (31.6295, -7.9811), 'Agadir': (30.4278, -9.5981),
    'Kenitra': (34.2610, -6.5802), 'Meknes': (33.8935, -5.5473),
    'Oujda': (34.6814, -1.9086), 'Tetouan': (35.5889, -5.3626),
}
CATEGORIES = ['epicerie', 'carburant', 'telecom', 'restauration', 'pharmacie', 'e-commerce']
START = datetime(2026, 11, 15, 8, tzinfo=timezone.utc)


def write_jsonl(path, rows):
    with path.open('w', encoding='utf-8', newline='\n') as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + '\n')


def generate(n, seed, output):
    if n < 200:
        raise ValueError('Utiliser au moins 200 evenements pour inclure les scenarios.')
    rng = random.Random(seed)
    output.mkdir(parents=True, exist_ok=True)
    merchants = []
    for i in range(1, 301):
        city = list(CITIES)[(i-1) % len(CITIES)]
        lat, lon = CITIES[city]
        cat = CATEGORIES[(i-1)//len(CITIES) % len(CATEGORIES)]
        merchants.append({'merchant_id':f'M-{i:03d}', 'name':f'{cat.title()} {i:03d}', 'city':city,
                          'category':cat, 'latitude':round(lat+rng.uniform(-.025,.025),6),
                          'longitude':round(lon+rng.uniform(-.025,.025),6)})
    write_jsonl(output/'merchants.json', merchants)
    duplicates = round(n*.02)
    base_count = n-duplicates
    events = []
    tx_num = 0
    truth = {'source':'synthetic fallback, not official professor files', 'seed':seed,
             'shared_device':{'device_id':'D-INJECT-SHARED','wallets':[]},
             'cycle':{'wallets':['W-CYCLE-A','W-CYCLE-B','W-CYCLE-C'],'tx_ids':[]},
             'collector':{'wallet':'W-COLLECTOR','tx_ids':[]}}

    def add_tx(payer, payee, typ, device, city, amount, statuses):
        nonlocal tx_num
        tx_num += 1
        txid = f'TX-{tx_num:08d}'
        common = {'tx_id':txid, 'type':typ, 'payer_wallet':payer, 'payee_id':payee,
                  'device_id':device, 'city':city, 'channel':'QR' if typ=='P2M' else 'APP',
                  'amount_mad':round(amount,2)}
        for status in statuses:
            t = START + timedelta(seconds=len(events)/20)
            events.append({**common, 'status':status, 'event_time':t.isoformat().replace('+00:00','Z')})
        return txid

    completed = ['INITIATED','AUTHORIZED','COMPLETED']
    for i in range(12):
        w=f'W-SHARED-{i:02d}'
        truth['shared_device']['wallets'].append(w)
        m=merchants[i]
        add_tx(w,m['merchant_id'],'P2M','D-INJECT-SHARED',m['city'],100+i,completed)
    for a,b in [('A','B'),('B','C'),('C','A')]:
        truth['cycle']['tx_ids'].append(add_tx(f'W-CYCLE-{a}',f'W-CYCLE-{b}','P2P',f'D-CYCLE-{a}','Rabat',500,completed))
    for i in range(30):
        truth['collector']['tx_ids'].append(add_tx(f'W-SENDER-{i:02d}','W-COLLECTOR','P2P',f'D-SENDER-{i:02d}','Rabat',10+i%10,completed))
    while len(events)<base_count:
        payer=f'W-{rng.randint(1,3500):05d}'
        typ=rng.choices(['P2M','P2P'],[.7,.3])[0]
        if typ=='P2M':
            merchant=rng.choice(merchants)
            payee,city=merchant['merchant_id'],merchant['city']
        else:
            payee=f'W-{rng.randint(1,3500):05d}'
            while payee==payer: payee=f'W-{rng.randint(1,3500):05d}'
            city=rng.choice(list(CITIES))
        states=rng.choices([completed,['INITIATED','REJECTED'],['INITIATED','AUTHORIZED','REJECTED'],completed+['REVERSED']], [84,9,5,2])[0]
        amount=round(rng.uniform(5,3500),2)
        # The final transaction may be incomplete at the end of the observation window.
        add_tx(payer,payee,typ,f'D-{payer[2:]}',city,amount,states[:base_count-len(events)])
    # Simulate delivery order separately from event time: selected events arrive late.
    late_indices=set(rng.sample(range(len(events)), round(n*.05)))
    deliveries=[]
    for i,row in enumerate(events):
        delay=rng.uniform(60,900) if i in late_indices else 0
        deliveries.append((i/20+delay,i,row))
    for j,i in enumerate(rng.sample(range(len(events)),duplicates)):
        arrival,_,row=deliveries[i]
        deliveries.append((arrival+rng.uniform(.01,3),len(events)+j,row.copy()))
    deliveries.sort(key=lambda x:(x[0],x[1]))
    rows=[r for _,_,r in deliveries]
    write_jsonl(output/'transactions_history.jsonl',rows)
    truth.update({'event_count':len(rows),'unique_event_keys':len(events),'duplicate_events':duplicates,
                  'late_original_events':len(late_indices),'merchant_count':len(merchants)})
    (output/'synthetic_truth.json').write_text(json.dumps(truth,indent=2,ensure_ascii=False)+'\n',encoding='utf-8')
    assert len(rows)==n and len(merchants)==300
    assert len({(r['tx_id'],r['status']) for r in rows})==base_count
    print(f'Evenements : {len(rows)} | commerçants : {len(merchants)}')
    print(f'Doublons ajoutes : {duplicates} | evenements originaux retardes : {len(late_indices)}')
    print(f'Fichiers generes dans : {output.resolve()}')


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode',choices=['file'],default='file',help='Jalon 1 batch uniquement')
    parser.add_argument('--n',type=int,default=50000)
    parser.add_argument('--seed',type=int,default=42)
    parser.add_argument('--output-dir',type=Path,default=Path('.'))
    args=parser.parse_args()
    generate(args.n,args.seed,args.output_dir)

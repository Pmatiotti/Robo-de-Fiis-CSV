insert into fund_registry (cnpj_fundo_classe, ticker, nome_fundo_classe)
values ('97.521.225/0001-25', 'MXRF11', 'Maxi Renda FII')
on conflict (cnpj_fundo_classe) do update
set ticker = excluded.ticker,
    nome_fundo_classe = excluded.nome_fundo_classe;

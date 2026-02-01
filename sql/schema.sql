create table if not exists fund_registry (
    cnpj_fundo_classe text primary key,
    ticker text not null,
    nome_fundo_classe text,
    created_at timestamptz not null default now(),
    unique (ticker)
);

create table if not exists fii_metrics (
    ticker text not null,
    data_referencia date not null,
    patrimonio_liquido numeric,
    valor_patrimonial_cota numeric,
    numero_cotistas integer,
    created_at timestamptz not null default now(),
    primary key (ticker, data_referencia)
);

create table if not exists fii_dividends (
    ticker text not null,
    data_referencia date not null,
    dividendo numeric,
    created_at timestamptz not null default now(),
    primary key (ticker, data_referencia)
);

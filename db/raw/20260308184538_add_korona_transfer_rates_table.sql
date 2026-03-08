-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS raw.korona_transfer_rates (
	raw_id bigserial NOT NULL,
	sending_currency_id int4 NOT NULL,
	receiving_currency_id int4 NOT NULL,
	load_ts timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	exchange_rate numeric(18, 6) NOT NULL,
	CONSTRAINT korona_transfer_rates_pkey PRIMARY KEY (raw_id),
	CONSTRAINT korona_transfer_rates_receiving_currency_id_fkey FOREIGN KEY (receiving_currency_id) REFERENCES md.currencies(id),
	CONSTRAINT korona_transfer_rates_sending_currency_id_fkey FOREIGN KEY (sending_currency_id) REFERENCES md.currencies(id)
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
SELECT 'down SQL query';
-- +goose StatementEnd
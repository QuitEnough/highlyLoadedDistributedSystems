CREATE TABLE IF NOT EXISTS devices (
                                       device_id TEXT NOT NULL,
                                       device_type TEXT NOT NULL,
                                       created_at BIGINT NOT NULL,
                                       meta TEXT,
                                       PRIMARY KEY (device_id)
    );

CREATE INDEX IF NOT EXISTS idx_device_id ON devices(device_id);

COMMENT ON TABLE devices IS 'Таблица для хранения устройств с поддержкой шардинга';
COMMENT ON COLUMN devices.device_id IS 'Уникальный идентификатор устройства (первичный ключ)';
COMMENT ON COLUMN devices.device_type IS 'Тип устройства';
COMMENT ON COLUMN devices.created_at IS 'Время регистрации устройства в формате UNIX timestamp (миллисекунды)';
COMMENT ON COLUMN devices.meta IS 'Метаданные устройства в формате JSON или другом текстовом формате';
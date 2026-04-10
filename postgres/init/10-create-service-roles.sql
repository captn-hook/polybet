DO
$$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'polybet_input') THEN
        CREATE ROLE polybet_input LOGIN PASSWORD 'polybet_input_dev_password';
    ELSE
        ALTER ROLE polybet_input WITH LOGIN PASSWORD 'polybet_input_dev_password';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'polybet_resolution') THEN
        CREATE ROLE polybet_resolution LOGIN PASSWORD 'polybet_resolution_dev_password';
    ELSE
        ALTER ROLE polybet_resolution WITH LOGIN PASSWORD 'polybet_resolution_dev_password';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'polybet_observe') THEN
        CREATE ROLE polybet_observe LOGIN PASSWORD 'polybet_observe_dev_password';
    ELSE
        ALTER ROLE polybet_observe WITH LOGIN PASSWORD 'polybet_observe_dev_password';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'polybet_experiment') THEN
        CREATE ROLE polybet_experiment LOGIN PASSWORD 'polybet_experiment_dev_password';
    ELSE
        ALTER ROLE polybet_experiment WITH LOGIN PASSWORD 'polybet_experiment_dev_password';
    END IF;
END
$$;

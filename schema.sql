-- ============================================================================
-- FreightIntel AI - PostgreSQL Schema
-- Run this against your PostgreSQL database before starting the backend.
-- ============================================================================
-- ============================================================================
-- TABLE 1: CARRIERS
-- ============================================================================
CREATE TABLE IF NOT EXISTS carriers (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    mc_number TEXT NOT NULL UNIQUE,
    dot_number TEXT NOT NULL,
    legal_name TEXT NOT NULL,
    dba_name TEXT,
    entity_type TEXT,
    status TEXT,
    email TEXT,
    phone TEXT,
    power_units TEXT,
    drivers TEXT,
    non_cmv_units TEXT,
    physical_address TEXT,
    mailing_address TEXT,
    date_scraped TEXT,
    mcs150_date TEXT,
    mcs150_mileage TEXT,
    operation_classification TEXT[],
    carrier_operation TEXT[],
    cargo_carried TEXT[],
    out_of_service_date TEXT,
    state_carrier_id TEXT,
    duns_number TEXT,
    safety_rating TEXT,
    safety_rating_date TEXT,
    basic_scores JSONB,
    oos_rates JSONB,
    insurance_policies JSONB,
    crashes JSONB,
    inspections JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
-- Create indexes for carriers table
CREATE INDEX IF NOT EXISTS idx_carriers_mc_number ON carriers(mc_number);
CREATE INDEX IF NOT EXISTS idx_carriers_dot_number ON carriers(dot_number);
CREATE INDEX IF NOT EXISTS idx_carriers_created_at ON carriers(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_carriers_status ON carriers(status);
-- Enable RLS for carriers
ALTER TABLE carriers ENABLE ROW LEVEL SECURITY;
-- RLS Policies for carriers table
DROP POLICY IF EXISTS "Enable read access for anonymous users" ON carriers;
DROP POLICY IF EXISTS "Enable all access for authenticated users" ON carriers;
CREATE POLICY "Enable read access for anonymous users" ON carriers
    FOR SELECT
    USING (true);
CREATE POLICY "Enable all access for authenticated users" ON carriers
    FOR ALL
    USING (true)
    WITH CHECK (true);
-- ============================================================================
-- TABLE 2: FMCSA_REGISTER
-- ============================================================================
CREATE TABLE IF NOT EXISTS fmcsa_register (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    number TEXT NOT NULL,
    title TEXT NOT NULL,
    decided TEXT,
    category TEXT,
    date_fetched TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(number, date_fetched)
);
-- Create indexes for fmcsa_register table
CREATE INDEX IF NOT EXISTS idx_fmcsa_register_number ON fmcsa_register(number);
CREATE INDEX IF NOT EXISTS idx_fmcsa_register_date_fetched ON fmcsa_register(date_fetched DESC);
CREATE INDEX IF NOT EXISTS idx_fmcsa_register_category ON fmcsa_register(category);
-- Enable RLS for fmcsa_register
ALTER TABLE fmcsa_register ENABLE ROW LEVEL SECURITY;
-- RLS Policies for fmcsa_register table
DROP POLICY IF EXISTS "Enable read access for anonymous users" ON fmcsa_register;
DROP POLICY IF EXISTS "Enable all access for authenticated users" ON fmcsa_register;
CREATE POLICY "Enable read access for anonymous users" ON fmcsa_register
    FOR SELECT
    USING (true);
CREATE POLICY "Enable all access for authenticated users" ON fmcsa_register
    FOR ALL
    USING (true)
    WITH CHECK (true);
-- ============================================================================
-- TABLE 3: USERS
-- ============================================================================
CREATE TABLE IF NOT EXISTS users (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    user_id TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    password_hash TEXT,
    role TEXT NOT NULL DEFAULT 'user' CHECK (role IN ('user', 'admin')),
    plan TEXT NOT NULL DEFAULT 'Free' CHECK (plan IN ('Free', 'Starter', 'Pro', 'Enterprise')),
    daily_limit INTEGER NOT NULL DEFAULT 50,
    records_extracted_today INTEGER NOT NULL DEFAULT 0,
    last_active TEXT DEFAULT 'Never',
    ip_address TEXT,
    is_online BOOLEAN DEFAULT false,
    is_blocked BOOLEAN DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
-- Create indexes for users table
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_users_user_id ON users(user_id);
CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);
-- Enable RLS for users
ALTER TABLE users ENABLE ROW LEVEL SECURITY;
-- RLS Policies for users table
DROP POLICY IF EXISTS "Enable read access for anonymous users" ON users;
DROP POLICY IF EXISTS "Enable all access for authenticated users" ON users;
CREATE POLICY "Enable read access for anonymous users" ON users
    FOR SELECT
    USING (true);
CREATE POLICY "Enable all access for authenticated users" ON users
    FOR ALL
    USING (true)
    WITH CHECK (true);
-- ============================================================================
-- TABLE 4: BLOCKED_IPS
-- ============================================================================
CREATE TABLE IF NOT EXISTS blocked_ips (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    ip_address TEXT NOT NULL UNIQUE,
    reason TEXT,
    blocked_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    blocked_by TEXT
);
-- Create indexes for blocked_ips table
CREATE INDEX IF NOT EXISTS idx_blocked_ips_ip ON blocked_ips(ip_address);
-- Enable RLS for blocked_ips
ALTER TABLE blocked_ips ENABLE ROW LEVEL SECURITY;
-- RLS Policies for blocked_ips table
DROP POLICY IF EXISTS "Enable all access for blocked_ips" ON blocked_ips;
DROP POLICY IF EXISTS "Enable read access for blocked_ips" ON blocked_ips;
CREATE POLICY "Enable all access for blocked_ips" ON blocked_ips
    FOR ALL
    USING (true)
    WITH CHECK (true);
CREATE POLICY "Enable read access for blocked_ips" ON blocked_ips
    FOR SELECT
    USING (true);
-- ============================================================================
-- TABLE 5: NEW_VENTURES (ALL BrokerSnapshot CSV columns)
-- ============================================================================
CREATE TABLE IF NOT EXISTS new_ventures (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    dot_number TEXT,
    prefix TEXT,
    docket_number TEXT,
    status_code TEXT,
    carship TEXT,
    carrier_operation TEXT,
    name TEXT,
    name_dba TEXT,
    add_date TEXT,
    chgn_date TEXT,
    common_stat TEXT,
    contract_stat TEXT,
    broker_stat TEXT,
    common_app_pend TEXT,
    contract_app_pend TEXT,
    broker_app_pend TEXT,
    common_rev_pend TEXT,
    contract_rev_pend TEXT,
    broker_rev_pend TEXT,
    property_chk TEXT,
    passenger_chk TEXT,
    hhg_chk TEXT,
    private_auth_chk TEXT,
    enterprise_chk TEXT,
    operating_status TEXT,
    operating_status_indicator TEXT,
    phy_str TEXT,
    phy_city TEXT,
    phy_st TEXT,
    phy_zip TEXT,
    phy_country TEXT,
    phy_cnty TEXT,
    mai_str TEXT,
    mai_city TEXT,
    mai_st TEXT,
    mai_zip TEXT,
    mai_country TEXT,
    mai_cnty TEXT,
    phy_undeliv TEXT,
    mai_undeliv TEXT,
    phy_phone TEXT,
    phy_fax TEXT,
    mai_phone TEXT,
    mai_fax TEXT,
    cell_phone TEXT,
    email_address TEXT,
    company_officer_1 TEXT,
    company_officer_2 TEXT,
    genfreight TEXT,
    household TEXT,
    metalsheet TEXT,
    motorveh TEXT,
    drivetow TEXT,
    logpole TEXT,
    bldgmat TEXT,
    mobilehome TEXT,
    machlrg TEXT,
    produce TEXT,
    liqgas TEXT,
    intermodal TEXT,
    passengers TEXT,
    oilfield TEXT,
    livestock TEXT,
    grainfeed TEXT,
    coalcoke TEXT,
    meat TEXT,
    garbage TEXT,
    usmail TEXT,
    chem TEXT,
    drybulk TEXT,
    coldfood TEXT,
    beverages TEXT,
    paperprod TEXT,
    utility TEXT,
    farmsupp TEXT,
    construct TEXT,
    waterwell TEXT,
    cargoothr TEXT,
    cargoothr_desc TEXT,
    hm_ind TEXT,
    bipd_req TEXT,
    cargo_req TEXT,
    bond_req TEXT,
    bipd_file TEXT,
    cargo_file TEXT,
    bond_file TEXT,
    owntruck TEXT,
    owntract TEXT,
    owntrail TEXT,
    owncoach TEXT,
    ownschool_1_8 TEXT,
    ownschool_9_15 TEXT,
    ownschool_16 TEXT,
    ownbus_16 TEXT,
    ownvan_1_8 TEXT,
    ownvan_9_15 TEXT,
    ownlimo_1_8 TEXT,
    ownlimo_9_15 TEXT,
    ownlimo_16 TEXT,
    trmtruck TEXT,
    trmtract TEXT,
    trmtrail TEXT,
    trmcoach TEXT,
    trmschool_1_8 TEXT,
    trmschool_9_15 TEXT,
    trmschool_16 TEXT,
    trmbus_16 TEXT,
    trmvan_1_8 TEXT,
    trmvan_9_15 TEXT,
    trmlimo_1_8 TEXT,
    trmlimo_9_15 TEXT,
    trmlimo_16 TEXT,
    trptruck TEXT,
    trptract TEXT,
    trptrail TEXT,
    trpcoach TEXT,
    trpschool_1_8 TEXT,
    trpschool_9_15 TEXT,
    trpschool_16 TEXT,
    trpbus_16 TEXT,
    trpvan_1_8 TEXT,
    trpvan_9_15 TEXT,
    trplimo_1_8 TEXT,
    trplimo_9_15 TEXT,
    trplimo_16 TEXT,
    total_trucks TEXT,
    total_buses TEXT,
    total_pwr TEXT,
    fleetsize TEXT,
    inter_within_100 TEXT,
    inter_beyond_100 TEXT,
    total_inter_drivers TEXT,
    intra_within_100 TEXT,
    intra_beyond_100 TEXT,
    total_intra_drivers TEXT,
    total_drivers TEXT,
    avg_tld TEXT,
    total_cdl TEXT,
    review_type TEXT,
    review_id TEXT,
    review_date TEXT,
    recordable_crash_rate TEXT,
    mcs150_mileage TEXT,
    mcs151_mileage TEXT,
    mcs150_mileage_year TEXT,
    mcs150_date TEXT,
    safety_rating TEXT,
    safety_rating_date TEXT,
    arber TEXT,
    smartway TEXT,
    tia TEXT,
    tia_phone TEXT,
    tia_contact_name TEXT,
    tia_tool_free TEXT,
    tia_fax TEXT,
    tia_email TEXT,
    tia_website TEXT,
    phy_ups_store TEXT,
    mai_ups_store TEXT,
    phy_mail_box TEXT,
    mai_mail_box TEXT,
    raw_data JSONB,
    scrape_date TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(dot_number, add_date)
);
-- Create indexes for new_ventures table
CREATE INDEX IF NOT EXISTS idx_new_ventures_dot_number ON new_ventures(dot_number);
CREATE INDEX IF NOT EXISTS idx_new_ventures_docket_number ON new_ventures(docket_number);
CREATE INDEX IF NOT EXISTS idx_new_ventures_add_date ON new_ventures(add_date);
CREATE INDEX IF NOT EXISTS idx_new_ventures_name ON new_ventures(name);
CREATE INDEX IF NOT EXISTS idx_new_ventures_phy_st ON new_ventures(phy_st);
CREATE INDEX IF NOT EXISTS idx_new_ventures_operating_status ON new_ventures(operating_status);
CREATE INDEX IF NOT EXISTS idx_new_ventures_created_at ON new_ventures(created_at DESC);
-- Enable RLS for new_ventures
ALTER TABLE new_ventures ENABLE ROW LEVEL SECURITY;
-- RLS Policies for new_ventures table
DROP POLICY IF EXISTS "Enable read access for anonymous users" ON new_ventures;
DROP POLICY IF EXISTS "Enable all access for authenticated users" ON new_ventures;
CREATE POLICY "Enable read access for anonymous users" ON new_ventures
    FOR SELECT
    USING (true);
CREATE POLICY "Enable all access for authenticated users" ON new_ventures
    FOR ALL
    USING (true)
    WITH CHECK (true);
-- ============================================================================
-- TRIGGER FUNCTIONS FOR AUTO-UPDATING TIMESTAMPS
-- ============================================================================
-- Function for carriers table
CREATE OR REPLACE FUNCTION update_carriers_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- Trigger for carriers table
DROP TRIGGER IF EXISTS update_carriers_updated_at ON carriers;
CREATE TRIGGER update_carriers_updated_at BEFORE UPDATE ON carriers
    FOR EACH ROW EXECUTE FUNCTION update_carriers_updated_at();
-- Function for fmcsa_register table
CREATE OR REPLACE FUNCTION update_fmcsa_register_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- Trigger for fmcsa_register table
DROP TRIGGER IF EXISTS update_fmcsa_register_updated_at ON fmcsa_register;
CREATE TRIGGER update_fmcsa_register_updated_at BEFORE UPDATE ON fmcsa_register
    FOR EACH ROW EXECUTE FUNCTION update_fmcsa_register_updated_at();
-- Function for users table
CREATE OR REPLACE FUNCTION update_users_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- Trigger for users table
DROP TRIGGER IF EXISTS update_users_updated_at ON users;
CREATE TRIGGER update_users_updated_at BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION update_users_updated_at();
-- Function for new_ventures table
CREATE OR REPLACE FUNCTION update_new_ventures_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- Trigger for new_ventures table
DROP TRIGGER IF EXISTS update_new_ventures_updated_at ON new_ventures;
CREATE TRIGGER update_new_ventures_updated_at BEFORE UPDATE ON new_ventures
    FOR EACH ROW EXECUTE FUNCTION update_new_ventures_updated_at();
-- ============================================================================
-- INSURANCE HISTORY TABLE (includes active + cancelled policies)
-- ============================================================================
CREATE TABLE IF NOT EXISTS insurance_history (
    id SERIAL PRIMARY KEY,
    docket_number VARCHAR(20),
    dot_number VARCHAR(20),
    ins_form_code VARCHAR(10),
    ins_type_desc VARCHAR(50),
    name_company VARCHAR(100),
    policy_no VARCHAR(50),
    trans_date VARCHAR(15),
    underl_lim_amount VARCHAR(15),
    max_cov_amount VARCHAR(15),
    effective_date VARCHAR(15),
    cancl_effective_date VARCHAR(15)
);

-- ============================================================================
-- INSPECTIONS TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS inspections (
    id SERIAL PRIMARY KEY,
    unique_id VARCHAR(50),
    report_number VARCHAR(50),
    report_state VARCHAR(10),
    dot_number VARCHAR(20),
    insp_date VARCHAR(20),
    insp_level_id VARCHAR(10),
    county_code_state VARCHAR(50),
    time_weight VARCHAR(10),
    driver_oos_total VARCHAR(10),
    vehicle_oos_total VARCHAR(10),
    total_hazmat_sent VARCHAR(10),
    oos_total VARCHAR(10),
    hazmat_oos_total VARCHAR(10),
    hazmat_placard_req VARCHAR(10),
    unit_type_desc TEXT,
    unit_make TEXT,
    unit_license VARCHAR(50),
    unit_license_state VARCHAR(10),
    vin VARCHAR(50),
    unit_decal_number VARCHAR(50),
    unit_type_desc2 TEXT,
    unit_make2 TEXT,
    unit_license2 VARCHAR(50),
    unit_license_state2 VARCHAR(10),
    vin2 VARCHAR(50),
    unit_decal_number2 VARCHAR(50),
    unsafe_insp VARCHAR(10),
    fatigued_insp VARCHAR(10),
    dr_fitness_insp VARCHAR(10),
    subt_alcohol_insp VARCHAR(10),
    vh_maint_insp VARCHAR(10),
    hm_insp VARCHAR(10),
    basic_viol VARCHAR(10),
    unsafe_viol VARCHAR(10),
    fatigued_viol VARCHAR(10),
    dr_fitness_viol VARCHAR(10),
    subt_alcohol_viol VARCHAR(10),
    vh_maint_viol VARCHAR(10),
    hm_viol VARCHAR(10),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_inspections_dot_number ON inspections (dot_number);
CREATE INDEX IF NOT EXISTS idx_inspections_report_number ON inspections (report_number);
CREATE INDEX IF NOT EXISTS idx_insurance_history_docket ON insurance_history (docket_number);
-- ============================================================================
-- INITIAL DATA
-- ============================================================================
-- Insert default admin user (only if not exists)
INSERT INTO users (user_id, name, email, role, plan, daily_limit, records_extracted_today, ip_address, is_online, is_blocked)
VALUES ('1', 'Admin User', 'wooohan3@gmail.com', 'admin', 'Enterprise', 100000, 0, '192.168.1.1', false, false)
ON CONFLICT (email) DO NOTHING;
-- ============================================================================
-- DOCUMENTATION
-- ============================================================================
COMMENT ON TABLE carriers IS 'FMCSA carrier data with insurance and safety information';
COMMENT ON COLUMN carriers.mc_number IS 'MC/MX Number - Unique identifier';
COMMENT ON COLUMN carriers.dot_number IS 'USDOT Number';
COMMENT ON COLUMN carriers.insurance_policies IS 'JSON array of insurance policies';
COMMENT ON COLUMN carriers.basic_scores IS 'JSON array of BASIC performance scores';
COMMENT ON COLUMN carriers.oos_rates IS 'JSON array of Out-of-Service rates';
COMMENT ON TABLE fmcsa_register IS 'FMCSA Daily Register entries with motor carrier decisions and notices';
COMMENT ON COLUMN fmcsa_register.number IS 'Docket number (e.g., MC-123456)';
COMMENT ON COLUMN fmcsa_register.title IS 'Entry title or description';
COMMENT ON COLUMN fmcsa_register.decided IS 'Date decided (MM/DD/YYYY format)';
COMMENT ON COLUMN fmcsa_register.category IS 'Category of decision (NAME CHANGE, REVOCATION, etc.)';
COMMENT ON COLUMN fmcsa_register.date_fetched IS 'Date when this entry was scraped';
COMMENT ON TABLE users IS 'User accounts for FreightIntel AI application';
COMMENT ON TABLE blocked_ips IS 'Blocked IP addresses for security';
COMMENT ON COLUMN users.user_id IS 'Application-level unique user ID';
COMMENT ON COLUMN users.role IS 'User role: user or admin';
COMMENT ON COLUMN users.plan IS 'Subscription plan: Free, Starter, Pro, Enterprise';
COMMENT ON COLUMN users.daily_limit IS 'Maximum MC records allowed per day';
COMMENT ON COLUMN users.is_blocked IS 'Whether the user is blocked from accessing the system';

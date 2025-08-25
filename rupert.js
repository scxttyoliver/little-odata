// Version 2.1.0
// Introduces a token that is shared between the proxy and client for managing the connection
// Token handling ensures that one client (instance) cannot have multiple connections

// ---------------------------------------------
// Helper Function Start
// ---------------------------------------------

// ---------------------------------------------
// Global Session Token Registry
// ---------------------------------------------

const odata_tokens = {};

// ---------------------------------------------
// Shared Helpers
// ---------------------------------------------

let odata_abort = new AbortController();

function encodeField(field)
{
	return '"' + field.replace(/"/g, '""') + '"';
}

function encodeValue(val)
{
	if (val instanceof Date) return val.toISOString();
	if (typeof val === 'string' && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$/.test(val)) return val;
	if (typeof val === 'string') return "'" + val.replace(/'/g, "''") + "'";
	if (typeof val !== 'number' && typeof val !== 'boolean') return "'" + String(val) + "'";
	return val;
}

function getHeaders(url, config)
{
	const proxy_url = config.proxy || "https://proxy.littleman.com.au";
	return { url: proxy_url + '/proxy?url=' + encodeURIComponent(url), headers: { "fm-username": config.username, "fm-password": config.password } };
}

function getURL(server, file, table, query_parts, limit)
{
	const base = 'https://' + server + '/fmi/odata/v4/' + file + '/' + table;
	const params = [];

	if (query_parts.field_select) params.push('$select=' + query_parts.field_select);
	if (query_parts.field_expand) params.push(query_parts.field_expand);
	if (query_parts.field_filter) params.push(query_parts.field_filter);
	if (limit) params.push('$top=' + limit);

	return params.length ? base + '?' + params.join('&') : base;
}

function loadSync(file, table)
{
	return JSON.parse(localStorage.getItem(file + '-' + table + '-sync')) || { serial_last: null, sync_timestamp: null, sync_complete: false };
}

function saveSync(file, table, state)
{
	localStorage.setItem(file + '-' + table + '-sync', JSON.stringify(state));
}

// ---------------------------------------------
// Query Construction
// ---------------------------------------------

function queryODATA(config_select, config_filter, table_base)
{
	function processFilter(field, condition)
	{
		const field_full = encodeField(field);
		if (Array.isArray(condition))
		{
			const clauses = condition.map(val => field_full + ' eq ' + encodeValue(val));
			return '(' + clauses.join(' or ') + ')';
		}
		if (typeof condition === 'object' && condition !== null)
		{
			const clauses = Object.entries(condition).map(([operator, val]) =>
			{
				return operator === 'contains' ? 'contains(' + field_full + ',' + encodeValue(val) + ')': field_full + ' ' + operator + ' ' + encodeValue(val);
			});
			return clauses.join(' and ');
		}
		return field_full + ' eq ' + encodeValue(condition);
	}

	const filter_parts = Object.entries(config_filter || {}).map(([f, c]) => processFilter(f, c));
	const filter_string = filter_parts.length ? '$filter=' + encodeURIComponent(filter_parts.join(' and ')) : '';

	let clause_select = [];
	let clause_expand = [];

	Object.keys(config_select).forEach(function (table)
	{
		const fields = Object.keys(config_select[table]).map(encodeField).join(',');
		if (table === table_base)
		{
			clause_select.push(fields);
		}
		else
		{
			clause_expand.push('"' + table + '"($select=' + fields + ')');
		}
	});

	return { field_select: encodeURIComponent(clause_select.join(',')),	field_expand: clause_expand.length ? '$expand=' + clause_expand.join(',') : '',	field_filter: filter_string	};
}

// ---------------------------------------------
// Fetch & Mapping
// ---------------------------------------------

async function callProxy(url, config, token)
{
	const proxy = getHeaders(url, config);
	try
	{
		const instance = config.instance || "default";
		if (token && odata_tokens[instance] !== token)
		{
			console.warn("Aborting due to session token mismatch");
			return;
		}

		const response = await fetch(proxy.url, { headers: proxy.headers, signal: odata_abort.signal });
		if (!response.ok) throw new Error("HTTPS: " + response.status);
		const data = await response.json();
		return data.value || data || [];
	}
	catch (error)
	{
	if (error.name === "AbortError" || error.message === "Load failed")
	{
		console.warn("ODATA fetch aborted by navigation or reload");
		callProxy.fetch_aborted = true;
	}
	return [];
	}
}

function compileMapping(config_select, table_base)
{
	const mapping = [];
	Object.keys(config_select).forEach(table =>
	{
		const fields = config_select[table];
		const path_source = (table === table_base) ? null : table;

		Object.keys(fields).forEach(field_fm =>
		{
			const rule = fields[field_fm];
			const key_target = typeof rule === 'object' && rule !== null ? rule.key : rule;
			const isArray = typeof rule === 'object' && rule.isArray;
			const delimiter = typeof rule === 'object' ? (rule.delimiter || '\n') : null;

			mapping.push({ field_fm, key_target, path_source, isArray, delimiter });
		});
	});
	return mapping;
}

function applyMapping(data, mapping_instruction)
{
	return data.map(record =>
	{
		let mapped = {};
		mapping_instruction.forEach(item =>
		{
			const source = item.path_source
				? (Array.isArray(record[item.path_source]) ? record[item.path_source][0] || {} : record[item.path_source] || {})
				: record;

			let value = source[item.field_fm];
			if (item.isArray && typeof value === 'string')
			{
				if (item.delimiter === '\n') value = value.replace(/\r\n|\r/g, '\n');
				mapped[item.key_target] = value.split(item.delimiter).map(v => v.trim()).filter(Boolean);
			}
			else
			{
				mapped[item.key_target] = item.field_fm in source ? value : null;
			}
		});
		return mapped;
	});
}

// ---------------------------------------------
// Count Query (Optional)
// ---------------------------------------------

async function getCount(config)
{
	const { table, file, server } = config;
	const instance = config.instance || "default";
	const session_token = Date.now().toString(36) + Math.random().toString(36).substring(2);
	odata_tokens[instance] = session_token;
	// Request only one record with the count field
	const count_field = config.mode.count_field || "_Count"; // Default fallback field
	const select_fields = { [table]: { [count_field]: count_field } };
	let count_filter = { ...config.filter };

	if (config.mode && config.mode.type === 'modified')
	{
		const modify_field = config.mode.modify_field || 'Timestamp Modify';
		const sync_state = loadSync(file, table);
		if (sync_state.sync_timestamp) count_filter[modify_field] = { ge: sync_state.sync_timestamp };
	}

	const query_parts = queryODATA(select_fields, count_filter, table);
	const url = getURL(server, file, table, query_parts, 1);

	const data = await callProxy(url, config, session_token);
	delete odata_tokens[instance];
	return Array.isArray(data) && data.length > 0 ? data[0][count_field] : 0;
}

// ---------------------------------------------
// Data Query
// ---------------------------------------------

async function getData(config)
{
	const { table, file, server, mode } = config;
	const instance = config.instance || "default";
	const session_token = Date.now().toString(36) + Math.random().toString(36).substring(2);
	odata_tokens[instance] = session_token;
	const limit = mode.limit || 10000;
	const cache = mode.cache !== false;
	const stream = mode.streaming !== false;
	const serial_field = (typeof config.mode.serial_field === 'string' && config.mode.serial_field.trim()) ? config.mode.serial_field.trim() : "_ID Serial";
	const modify_field = config.mode.modify_field || 'Timestamp Modify'

	// Always ensure the serial field is included in select
	if (!config.select[table][serial_field]) config.select[table][serial_field] = serial_field;

	const map_compiled = compileMapping(config.select, table);
	const base_filter = { ...config.filter };

	let sync_state = cache ? loadSync(file, table) : {};
	let serial_last = cache && sync_state.serial_last != null ? Number(sync_state.serial_last) : null;
	const sync_type = cache ? (!sync_state.sync_complete ? 'full' : 'modified') : 'live';
	const timestamp_start = new Date();

	// Only set sync timestamp once, when starting full sync for the first time
	if (sync_type === 'full' && !sync_state.sync_timestamp)
	{
		sync_state.sync_timestamp = timestamp_start;
		saveSync(file, table, sync_state);
	}

	// Add timestamp filter for modified mode
	if (sync_type === 'modified' && sync_state.sync_timestamp) base_filter[modify_field] = { ge: sync_state.sync_timestamp };

	let results = [];
	let put_queue = [];
	let fetched;
	let serial_paging = false;

	do
	{
		if (odata_abort.signal.aborted)
		{
			console.warn("Fetch aborted during retrieval");
			break;
		}

		let batch_filter = { ...base_filter };

		// Full mode always page by serial, modified only after first page
		if (serial_last !== null && (sync_type === 'full' || serial_paging))
		{
			batch_filter[serial_field] = { gt: serial_last };
		}

		const query_parts = queryODATA(config.select, batch_filter, table);
		const url = getURL(server, file, table, query_parts, limit);

		fetched = await callProxy(url, config, session_token);
		if (!fetched.length) break;

		const mapped = applyMapping(fetched, map_compiled);

		if (cache)
		{
			// Only promise when not caching due to order of operations when navigating to and from
			if (typeof putData === 'function') await putData(mapped);

			if (sync_type === 'full')
			{
				sync_state.serial_last = serial_last;
				saveSync(file, table, sync_state);
			}

			if (sync_type === 'modified' && fetched.length === limit)
			{
				serial_paging = true;
			}
		}
		else
		{
			if (typeof putData === 'function') put_queue.push(putData(mapped));
			if (!stream) results.push(...mapped);
		}

		let highest = serial_last;
		for (let i = 0; i < fetched.length; i++)
		{
			const val = Number(fetched[i][serial_field]);
			if (isFinite(val) && (highest == null || val > highest)) highest = val;
		}

		if (highest === serial_last)
		{
			console.warn("No serial progression detected");
			break;
		}

		serial_last = highest;

	} while (fetched.length === limit);
	
	if ( !cache ) await Promise.all(put_queue);

	if (cache && sync_type === 'full' && fetched.length < limit && callProxy.fetch_aborted === false)
	{
		sync_state.sync_complete = true;
		saveSync(file, table, sync_state);
	}
	else if (cache && sync_type === 'modified')
	{
		// Only update timestamp after modified sync finishes
		sync_state.sync_timestamp = timestamp_start;
		saveSync(file, table, sync_state);
	}

	if (cache && sync_type === 'modified' && callProxy.fetch_aborted === false && typeof window.putRecount === 'function')
	{
		// Recount hook for consistency check
		const server_recount = await getCount(config);
		window.putRecount(server_recount);
	}

	delete odata_tokens[instance];
	if (typeof onSync === 'function') onSync(sync_type);
	return !stream && !cache ? results : undefined;
}


// ---------------------------------------------
// Unified Entry Point
// ---------------------------------------------

async function callODATA(config)
{
	console.time('ODATA Retrieval');
	callProxy.fetch_aborted = false;
	let server_count = 0;

	// Optional count query
	if (config.mode.count === true)
	{
		server_count = await getCount(config);
		if (typeof window.putRange === 'function') window.putRange(server_count);
	}

	const data = await getData(config);
	console.timeEnd('ODATA Retrieval');
	return { output: data, count: server_count };
}

// ==============================
// Helper Function Stop
// ==============================

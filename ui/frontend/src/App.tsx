import './App.css'

import logo from './assets/clp-logo.png';

import {
    useEffect,
    useState,
    forwardRef,
} from 'react'

import {
    Box,
    Chip,
    Link,
} from '@mui/joy'

import Divider, { dividerClasses } from '@mui/material/Divider';
import TransitionProps from '@mui/material/transitions';

import {
    Dialog,
    Slide,
    AppBar,
    Toolbar,
    IconButton,
    Typography,
} from '@mui/material';

import CloseIcon from '@mui/icons-material/Close';
import BarChartIcon from '@mui/icons-material/BarChart';

import { 
    DataGrid, 
    GridColDef, 
    GridCellParams,
    useGridApiRef,
} from '@mui/x-data-grid';

import { BarChart } from '@mui/x-charts/BarChart';

type TransitionProps = typeof TransitionProps;
const Transition = forwardRef(function Transition(
    props: TransitionProps & {
      children: React.ReactElement<unknown>;
    },
        ref: React.Ref<unknown>,
    ) {
        return <Slide direction="up" ref={ref} {...props} />;
    }
); 

const TYPE = ['', 'unstructured', 'semiStructured'];
const METRIC = ['', 'hotRun', 'coldRun'];

type BenchmarkingResultBasic<T> = {
    [type: string]: {
        [metric: string]: T;
    };
}
function BenchmarkingResultBasicInitializer<T>(cb: (type: string, metric: string) => T) {
    return TYPE.reduce((typeAcc, type) => {
        if (type) {
            typeAcc[type] = METRIC.reduce((metricAcc, metric) => {
                if (metric) {
                    metricAcc[metric] = cb(type, metric);
                }
                return metricAcc;
            }, {} as {
                [metric: string]: T;
            })
        }
        return typeAcc;
    }, {} as BenchmarkingResultBasic<T>);
}

const COLUMNS: BenchmarkingResultBasic<{
    [field_name: string]: GridColDef;
}> = BenchmarkingResultBasicInitializer((type, metric) => {
    return {
        'misc': {   
            field: 'misc', 
            headerName: '', 
            width: 300, 
            align: 'right', 
            sortable: false,
            renderCell: (params: GridCellParams) => <MetricCell params={params} type={type} metric={metric} />,
        }
    };
});

const TARGETS: BenchmarkingResultBasic<Map<string, string>> = BenchmarkingResultBasicInitializer(() => {
    return new Map<string, string>()
});
const TARGET_ORDERS: BenchmarkingResultBasic<Map<string, number>> = BenchmarkingResultBasicInitializer(() => {
    return new Map<string, number>();
});

const BENCHMARK_WORKLOAD: BenchmarkingResultBasic<{
    name: string;
    size: number;
}> = BenchmarkingResultBasicInitializer((type, _) => {
    if (TYPE[1] == type) {
        return {
            name: 'Hadoop (258GB)',
            size: 276224164352,
        };
    } else if (TYPE[2] == type) {
        return { 
            name: 'MongoDB (64GB)',
            size: 69582861765,
        };
    }
    return { 
        name: 'ERROR',
        size: 0,
    };
})

const MetricCell = ({ params, type, metric }: { params: GridCellParams, type: string, metric: string }) => {
    const [openChartDialog, setOpenChartDialog] = useState(false);

    const extractNumber = (value: string) => {
        if (value) {
            return parseFloat(value.replace(/[^\d.-]/g, '')); // Remove non-numeric characters
        }
        return null;
    };
    const relevantFields = Array.from(TARGETS[type][metric].keys()).filter((f) => 0 != extractNumber(params.row[f]));
    const values = relevantFields.map((field) => extractNumber(params.row[field])).filter((v) => null !== v);
    const relevantFieldDisplayNames = relevantFields.map((field) => TARGETS[type][metric].get(field));

    const handleClickOpen = () => {
        setOpenChartDialog(true);
    }

    const handleClose = () => {
        setOpenChartDialog(false);
    }

    return (
        <div>
            <IconButton
                edge="start"
                color="inherit"
                onClick={handleClickOpen}
                aria-label="barchart"
            >
                <BarChartIcon />
            </IconButton>
            <Dialog
                fullScreen
                open={openChartDialog}
                onClose={handleClose}
                TransitionComponent={Transition}
            >
                <AppBar sx={{ position: 'relative' }}>
                    <Toolbar>
                        <IconButton
                            edge="start"
                            color="inherit"
                            onClick={handleClose}
                            aria-label="true"
                        >
                            <CloseIcon />
                        </IconButton>
                        <Typography sx={{ ml: 2, flex: 1 }} variant="h6" component="div">
                            {params.row['misc']}
                        </Typography>
                    </Toolbar>
                </AppBar>
                <BarChart
                    series={[
                        { data: values },
                    ]}
                    xAxis={[{ data: relevantFieldDisplayNames, scaleType: 'band' }]}
                >
                    
                </BarChart>
            </Dialog>
            {params.value as string}
        </div>
    );
}

const ColoredCell = ({ params, type, metric }: { params: GridCellParams, type: string, metric: string }) => {
    const extractNumberAndUnit = (value: string) => {
        if (value) {
            const number = parseFloat(value.replace(/[^\d.-]/g, '')); // Extract the number
            const unit = value.replace(/[\d.-]/g, '').trim(); // Extract the unit by removing numeric characters
            return { number: number, unit: unit };
        }
        return { number: 0, unit: '' };
    };
    
    const relevantFields = Array.from(TARGETS[type][metric].keys());
    const values = relevantFields.map((field) => extractNumberAndUnit(params.row[field]).number);

    // Check if at least two fields have values to perform the comparison
    const nonEmptyValues = values.filter((v) => 0 != v);
    if (nonEmptyValues.length < 2) {
        return (
            <div
                style={{
                    backgroundColor: 'white',
                    color: 'black',
                    paddingRight: '10px',
                }}
            >
                {params.value as string}
            </div>
        );
    }

    const { number: currentValue, unit: currentUnit } = extractNumberAndUnit(params.value as string);
    const reverse = ('MB/s' === currentUnit);  // If it is speed, then the greater the value is, the better it is
    let backgroundColor = 'white';
    let redValue = 0;
    let greenValue = 255;
    let textColor = 'black';

    if (0 != currentValue) {
        const minValue = Math.min(...nonEmptyValues);
        const maxValue = Math.max(...nonEmptyValues);
        
        // The color changes from green (0, 255, 0) to yellow (255, 255, 0) to red (255, 0, 0) to dark (0, 0, 0).
        // So there are three stages.

        const ratio = reverse ? (maxValue - currentValue) / (maxValue - minValue)
                            : (currentValue - minValue) / (maxValue - minValue);
        if (0.3333 > ratio) {
            // Change from green (0, 255, 0) to yellow (255, 255, 0)
            redValue = 255 * ratio / 0.3333;
            greenValue = 255;
        } else if (0.6666 > ratio) {
            // Change from yellow (255, 255, 0) to red (255, 0, 0)
            redValue = 255;
            greenValue = 255 * (1 - (ratio - 0.3333) / 0.3333);
        } else {
            // Change from red to dark (255, 0, 0) to dark (0, 0, 0)
            redValue = 255 * (1 - (ratio - 0.6666) / 0.3333);
            greenValue = 0
        }
    
        backgroundColor = `rgb(${redValue}, ${greenValue}, 0)`;
    
        const luminance = (r: number, g: number, b: number) => 0.2126 * r + 0.7152 * g + 0.0722 * b;
    
        const lum = luminance(redValue, greenValue, 0); // Use the background color's RGB values
    
        textColor = lum < 128 ? 'white' : 'black';
    } else {
        params.value = '';
    }

    return (
        <div
            style={{
                backgroundColor,
                color: textColor,
                paddingRight: '10px',
            }}
        >
            {params.value as string}
        </div>
    );
};

type BenchmarkingResultResponse = {
    message: string;
    payload: {
        target: string;
        target_displayed_name: string,
        displayed_order: number,
        is_enable: boolean,
        type: number;
        metric: number;
        ingest_time: number;
        compressed_size: number;
        avg_ingest_mem: number;
        avg_query_mem: number;
        query_times: string;
    }[];
};

type BenchmarkingResultRow = BenchmarkingResultBasic<{
    [target: string]: string;
}>;
function RowInitializer(misc: string) {
    return BenchmarkingResultBasicInitializer<{
        [target: string]: string;
    }>(() => {
        return {
            'misc': misc
        };
    });
}

const INGEST_TIME_ROW: BenchmarkingResultRow = RowInitializer('Ingest time:');
const INGEST_SPEED_ROW: BenchmarkingResultRow = RowInitializer('Ingest speed:');
const COMPRESSED_SIZE_ROW: BenchmarkingResultRow = RowInitializer('Compressed size:');
const MEM_USAGE_INGEST_ROW: BenchmarkingResultRow = RowInitializer('Mem usage (ingest):');
const MEM_USAGE_QUERY_ROW: BenchmarkingResultRow = RowInitializer('Mem usage (query):');
const AVG_QUERY_LATENCY_ROW: BenchmarkingResultRow = RowInitializer('Avg query latency:');
const QUERY_LATENCY_ROWS: BenchmarkingResultBasic<{
    [target: string]: string;
}[]> = BenchmarkingResultBasicInitializer(() => []);
const NR_QUERIES: BenchmarkingResultBasic<number> = BenchmarkingResultBasicInitializer(() => {
    return 0;
})

function GetTime(time: number) {
    // ms -> s
    const formattedTime = (time / 1000).toFixed(2);
    return `${new Intl.NumberFormat().format(Number(formattedTime))}s`;
}

function GetSize(size: number) {
    // B -> MB
    const formattedSize = (size / 1024 / 1024).toFixed(2);
    return `${new Intl.NumberFormat().format(Number(formattedSize))}MB`;
}

function GetSpeed(size: number, time: number) {
    // Original data size / Ingest time
    // (B, ms) -> (MB/s)
    let formattedSpeed;
    if (0 == time) {
        formattedSpeed = 0;
    } else {
        formattedSpeed = ((size / 1024 / 1024) / (time / 1000)).toFixed(2);
    }
    return `${new Intl.NumberFormat().format(Number(formattedSpeed))}MB/s`;
}

function GetRows(type: string, metric: string) {
    const rows = [
        { id: 1, ...COMPRESSED_SIZE_ROW[type][metric] },
        { id: 2, ...MEM_USAGE_INGEST_ROW[type][metric] },
        { id: 3, ...MEM_USAGE_QUERY_ROW[type][metric] },
        { id: 4, ...INGEST_TIME_ROW[type][metric] },
        { id: 5, ...INGEST_SPEED_ROW[type][metric] },
        { id: 6, ...AVG_QUERY_LATENCY_ROW[type][metric] },
    ]
    for (let i = 0; i < NR_QUERIES[type][metric]; i++) {
        rows.push(
            { id: 7 + i, ...QUERY_LATENCY_ROWS[type][metric][i] }
        );
    }
    return rows;
}

function App() {
    const [type, setType] = useState(TYPE[1]);
    const [benchmarkWorkload, setBenchmarkWorkload] = useState<string>();
    const [metric, setMetric] = useState(METRIC[1]);
    const [columns, setColumns] = useState<GridColDef[]>([]);
    const [noRowsPrompt, setNoRowsPrompt] = useState("");

    const apiRef = useGridApiRef();

    const NoRowsOverlay = () => (
        <Box
            alignItems={"center"}
            display={"flex"}
            height={"100%"}
            justifyContent={"center"}
        >
            {noRowsPrompt}
        </Box>
    );

    useEffect(() => {
        apiRef.current.setRows([]);
        const fetchData = async () => {
            try {
                setNoRowsPrompt('Loading')
                const response = await fetch(`api/get`);
                const result: BenchmarkingResultResponse = await response.json();
                for (let i = 0; i < result.payload.length; i++) {
                    if (!result.payload[i].is_enable) {
                        continue;
                    }
                    const target: string = result.payload[i].target;
                    const type: string = TYPE[result.payload[i].type];
                    const metric: string = METRIC[result.payload[i].metric];
                    const query_times_arr: number[] = JSON.parse(result.payload[i].query_times);
                    if (0 == NR_QUERIES[type][metric]) {
                        NR_QUERIES[type][metric] = query_times_arr.length;
                        for (let j = 0; j < NR_QUERIES[type][metric]; j++) {
                            QUERY_LATENCY_ROWS[type][metric].push({
                                'misc': `Q${j + 1}:`
                            });
                        }
                    } else if (0 == query_times_arr.length || query_times_arr.length != NR_QUERIES[type][metric]) {
                        continue
                    }
                    // Calculate the average query latency
                    const sumOfQueryLatencies = query_times_arr.reduce((acc, latency) => acc  + latency, 0); 
                    AVG_QUERY_LATENCY_ROW[type][metric] = {
                        ...AVG_QUERY_LATENCY_ROW[type][metric],
                        [target]: GetTime(sumOfQueryLatencies / query_times_arr.length),
                    };
                    for (let j = 0; j < query_times_arr.length; j++) {
                        QUERY_LATENCY_ROWS[type][metric][j] = {
                            ...QUERY_LATENCY_ROWS[type][metric][j],
                            [target]: GetTime(query_times_arr[j]),
                        }
                    }           
                    TARGETS[type][metric].set(target, result.payload[i].target_displayed_name);
                    TARGET_ORDERS[type][metric].set(target, result.payload[i].displayed_order);
                    INGEST_TIME_ROW[type][metric] = {
                        ...INGEST_TIME_ROW[type][metric],
                        [target]: GetTime(result.payload[i].ingest_time),
                    };
                    INGEST_SPEED_ROW[type][metric] = {
                        ...INGEST_SPEED_ROW[type][metric],
                        [target]: GetSpeed(BENCHMARK_WORKLOAD[type][metric].size, result.payload[i].ingest_time),
                    }
                    COMPRESSED_SIZE_ROW[type][metric] = {
                        ...COMPRESSED_SIZE_ROW[type][metric],
                        [target]: GetSize(result.payload[i].compressed_size),
                    };
                    MEM_USAGE_INGEST_ROW[type][metric] = {
                        ...MEM_USAGE_INGEST_ROW[type][metric],
                        [target]: GetSize(result.payload[i].avg_ingest_mem),
                    };
                    MEM_USAGE_QUERY_ROW[type][metric] = {
                        ...MEM_USAGE_QUERY_ROW[type][metric],
                        [target]: GetSize(result.payload[i].avg_query_mem),
                    };
                }
                for (let i = 1; i < TYPE.length; i++) {
                    for (let j = 1; j < METRIC.length; j++) {
                        // Sort the targets based on given order
                        const targetMap = TARGETS[TYPE[i]][METRIC[j]];
                        const targetOrderMap = TARGET_ORDERS[TYPE[i]][METRIC[j]];

                        // Convert entries of targetMap to an array and sort them based on targetOrderMap values
                        const sortedEntries = Array.from(targetMap.entries()).sort((a, b) => {
                            const orderA = targetOrderMap.get(a[0]);
                            const orderB = targetOrderMap.get(b[0]);

                            // If order values exist, sort by those values. Otherwise, maintain original order
                            if (orderA !== undefined && orderB !== undefined) {
                                return orderA - orderB;
                            }
                            return 0;
                        });

                        // Create a new Map with the sorted entries
                        const sortedTargetMap = new Map<string, string>(sortedEntries);
                        TARGETS[TYPE[i]][METRIC[j]] = sortedTargetMap;
                        // Build columns
                        TARGETS[TYPE[i]][METRIC[j]].forEach((value: string, key: string)=> {
                            COLUMNS[TYPE[i]][METRIC[j]] = {
                                ...COLUMNS[TYPE[i]][METRIC[j]],
                                [key]: {
                                    field: key,
                                    headerName: value,
                                    width: 200,
                                    align: 'right',
                                    sortable: false,
                                    renderCell: (params: GridCellParams) => <ColoredCell params={params} type={TYPE[i]} metric={METRIC[j]} />, 
                                },
                            }
                        });
                    }
                }
                setColumns(Object.values(COLUMNS[type][metric]));
                const rowsV3 = GetRows(type, metric);
                for (let i = 0; i < rowsV3.length; i++) {
                    apiRef.current.updateRows([rowsV3[i]]);
                }
                setNoRowsPrompt('')
            } catch (error) {
                console.error(error);
                setNoRowsPrompt('Network error');
            }
        }
        fetchData();
    }, [])

    useEffect(() => {
        setColumns(Object.values(COLUMNS[type][metric]));
        const rows = GetRows(type, metric);
        apiRef.current.setRows([]);
        for (let i = 0; i < rows.length; i++) {
            apiRef.current.updateRows([rows[i]]);
        }
        setBenchmarkWorkload(BENCHMARK_WORKLOAD[type][metric].name);
    }, [type, metric]);

    return (
        <Box
            display={'flex'}
            flexDirection={'column'}
            height={'100%'}
            width={'100%'}
            sx={{ marginLeft: 2 }}
        >
            <img src={logo} alt="CLPBench" style={{ width: '100px', height: 'auto', marginTop: '15px', marginBottom: '10px' }} />
            <Box
                sx={{
                    display: 'flex',
                    alignItems: 'center',
                    bgcolor: 'background.paper',
                    color: 'text.secondary',
                    '& svg': {
                      m: 2,
                    },
                    [`& .${dividerClasses.root}`]: {
                      mx: 0.5,
                      borderWidth: '1px',
                    },
                  }}
            >
                <Link href='https://github.com/y-scope/clp-bench-prototype/blob/main/docs/methodology.md'>Methodology</Link>
                <Divider orientation="vertical" flexItem />
                <Link href='https://docs.yscope.com/clp/main/user-guide/core-unstructured/clp.html'>CLP Documentation</Link>
            </Box>
            <Box
                role="group"
                aria-labelledby="fav-movie"
                sx={{ display: 'flex', flexWrap: 'wrap', gap: 1, marginBottom: 2, marginTop: 2, }}
            >
                <span>Type: </span>
                <Chip
                    color={ 'unstructured' === type ? 'success' : 'neutral' }
                    onClick={() => {
                        setType('unstructured')
                    }}
                    variant="solid"
                >
                    Unstructured
                </Chip>
                <Chip
                    color={ 'semiStructured' === type ? 'success' : 'neutral' }
                    onClick={() => {
                        setType('semiStructured')
                    }}
                    variant="solid"
                >
                    Semi-Structured
                </Chip>
            </Box>
            <Box
                role="group"
                aria-labelledby="fav-movie"
                sx={{ display: 'flex', flexWrap: 'wrap', gap: 1, marginBottom: 2, }}
            >
                <span>Metric: </span>
                <Chip
                    color={ 'hotRun' === metric ? 'success' : 'neutral' }
                    onClick={() => {
                        setMetric('hotRun')
                    }}
                    variant="solid"
                >
                    Hot Run
                </Chip>
                <Chip
                    color={ 'coldRun' === metric ? 'success' : 'neutral' }
                    onClick={() => {
                        setMetric('coldRun')
                    }}
                    variant="solid"
                >
                    Cold Run
                </Chip>
            </Box>
            <span>The used benchmark workload: {benchmarkWorkload}</span>
            <DataGrid 
                hideFooter={true}
                apiRef={apiRef}
                columns={columns}
                slots={{noRowsOverlay: NoRowsOverlay}} 
            />
        </Box>
    )
}

export default App

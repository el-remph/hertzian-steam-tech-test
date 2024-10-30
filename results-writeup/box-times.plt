$cython_asyncio << EOF
22.862
22.715
23.116
22.645
20.350
20.736
20.552
20.112
19.728
19.989
25.910
20.935
19.733
23.194
21.306
20.437
24.446
23.394
23.718
23.215
EOF

$asyncio << EOF
26.102
24.551
22.601
24.267
23.979
23.509
23.009
25.365
24.700
23.720
24.734
21.002
22.290
23.112
23.649
21.675
21.202
21.957
21.401
21.225
EOF

$cython_asyncio_bc << EOF
22.746
23.161
23.044
20.724
21.030
20.889
23.425
20.298
21.401
19.818
23.346
20.367
21.738
20.871
20.283
21.055
22.074
21.332
22.575
21.001
EOF

set style data boxplot
set grid
set key outside center top
set ylabel 'time (seconds)'
plot	$cython_asyncio	using (2.0):1	title 'cython-asyncio',	\
	$asyncio	using (1.0):1	title 'asyncio (byte-compiled)',	\
	$cython_asyncio_bc	using (3.0):1	title 'cython-asyncio (byte-compiled starting script)'

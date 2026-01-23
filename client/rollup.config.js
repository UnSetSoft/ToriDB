import resolve from '@rollup/plugin-node-resolve'
import commonjs from '@rollup/plugin-commonjs'


const toridb = [
  // ESM BUILD (para bundlers)
  {
    input: './src/sdk.js',
    output: {
      file: './dist/toridb.esm.js',
      format: 'esm',
      sourcemap: true,
    },
    plugins: [resolve({ extensions: ['.js', '.mjs'] }), commonjs()],
  },
]

export default toridb
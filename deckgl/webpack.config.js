const CONFIG = {
  mode: 'development',
  entry: { app: './app.js' },
  output: { library: 'App' },
  module: {
    rules: [
      { test: /\.js$/, loader: 'babel-loader', exclude: [/node_modules/], options: { presets: ['@babel/preset-react'] } }
    ]
  },
  devServer: {
    host: '0.0.0.0',
    port: 8080,
    disableHostCheck: true,
    hot: true
  }
};


module.exports = env => (env ? require('../../webpack.config.local')(CONFIG)(env) : CONFIG);

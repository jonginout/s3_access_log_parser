const s3alp = require('s3-access-log-parser')
const AwsS3 = require ('aws-sdk/clients/s3')
const fs = require('fs')

const s3 = new AwsS3 ({
  accessKeyId: '******************',
  secretAccessKey: '*************************************************',
  region: 'ap-northeast-2',
})
const BUCKET_NAME = 'BUCKET_NAME'

const getListObject = (
    date,
    dir = 'BUCKET_DIR'
) => {
    const s3params = {
        Bucket: BUCKET_NAME,
        Prefix: `${dir}/${date}`
    }

    return new Promise ((resolve, reject) => {
        s3.listObjectsV2 (s3params, (err, data) => {
            if (err) return reject (err)
            return resolve (data.Contents)
        })
    })
}

const getObject = (
    filename
) => {

    const s3params = {
        Bucket: BUCKET_NAME,
        Key: filename
    }

    return new Promise ((resolve, reject) => {
        s3.getObject(s3params, (err, data) => {
            if (err) return reject(err)
            const resultData = data.Body.toString('utf-8')
            if(!resultData) console.log(`NULL FILE!!`)
            return resolve(resultData)
        })
    })
}

const getDates = (date) => {
    const now = (date) => {
        if(!date) return new Date()
        return new Date(date)
    }
    let result = [
        new Date(now(date).setDate(now(date).getDate()-1)),
        now(date),
        new Date(now(date).setDate(now(date).getDate()+1))
    ]
    result = result.map(date => {
        return `${date.getFullYear()}-${date.getMonth().toString().length < 2 ? '0'+(date.getMonth()+1) : (date.getMonth()+1)}-${date.getDate().toString().length < 2 ? '0'+date.getDate() : date.getDate()}`
    })
    return result
}



const run = (date) => {
    let fileList = null
    let index = 0
    let dates
    
    if(date) dates = getDates(date)
    else dates = getDates()

    const yesterday = dates[0]
    const today = dates[1]
    const tomorrow = dates[2]
    
    const parser = async () => {
        if(!fileList){
            await Promise.all([
                getListObject(yesterday),
                getListObject(today),
                getListObject(tomorrow)
            ]).then(result => {
                fileList = (result[0].concat(result[1])).concat(result[2])
            })
        }
        if(fileList.length < index) return process.exit(1)
    
        let result = await getObject(fileList[index].Key)
        // result = s3alp(result) ? JSON.stringify(s3alp(result)) : result
    
        const DIR = `result_files/s3_access_log/${BUCKET_NAME}`
        if (!fs.existsSync(DIR)) fs.mkdirSync(DIR)
        fs.appendFile(
            `${DIR}/${today}_s3_access_log.json`, 
            `${result}\n`,
            'utf8',
            (err) => {
                if(err) console.error(err)
            }
        )
    
        console.log(`${index}번째 처리..`)
        index++
        parser()
    }

    parser()
}

run()
// run('2019-05-01')

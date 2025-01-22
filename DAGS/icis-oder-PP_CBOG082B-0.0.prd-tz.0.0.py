from datetime import datetime, timedelta
from kubernetes.client import models as k8s
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.helpers import chain, cross_downstream
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
import pendulum
local_tz = pendulum.timezone("Asia/Seoul")
import sys
sys.path.append('/opt/bitnami/airflow/dags/git_sa-common')

from icis_common import *
COMMON = ICISCmmn(DOMAIN='oder',ENV='prd-tz', NAMESPACE='t-order'
                , WORKFLOW_NAME='PP_CBOG082B',WORKFLOW_ID='7421b821cfc04f5e9960abae5b4ab4a1', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOG082B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 7, 10, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('7421b821cfc04f5e9960abae5b4ab4a1')

    cbog082bJob_vol = []
    cbog082bJob_volMnt = []
    cbog082bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    cbog082bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    cbog082bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbog082bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cbog082bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbog082bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'c22f20e18b0e40618c79a6dc8ab4a3a3',
        'volumes': cbog082bJob_vol,
        'volume_mounts': cbog082bJob_volMnt,
        'env_from':cbog082bJob_env,
        'task_id':'cbog082bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.132',
        'arguments':["--job.name=cbog082bJob", "jobFlag=1", "fromDate=${YYYYMMDD,YY,-7}", "toDate=${YYYYMMDD}", "ver="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('7421b821cfc04f5e9960abae5b4ab4a1')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbog082bJob,
        Complete
    ]) 

    # authCheck >> cbog082bJob >> Complete
    workflow









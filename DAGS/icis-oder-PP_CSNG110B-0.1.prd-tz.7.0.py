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
                , WORKFLOW_NAME='PP_CSNG110B',WORKFLOW_ID='64c250b43dba488ba76fdfb038f9010e', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG110B-0.1.prd-tz.7.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 12, 30, 17, 42, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('64c250b43dba488ba76fdfb038f9010e')

    csng110bJob_vol = []
    csng110bJob_volMnt = []
    csng110bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    csng110bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc',' /app/order'))

    csng110bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng110bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng110bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng110bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '41eb94d6ccfb418088c1e777a265605f',
        'volumes': csng110bJob_vol,
        'volume_mounts': csng110bJob_volMnt,
        'env_from':csng110bJob_env,
        'task_id':'csng110bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.5',
        'arguments':["--job.name=csng110bJob", "workDay=${YYYYMMDD}", "empNo=91128346"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('64c250b43dba488ba76fdfb038f9010e')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng110bJob,
        Complete
    ]) 

    # authCheck >> csng110bJob >> Complete
    workflow









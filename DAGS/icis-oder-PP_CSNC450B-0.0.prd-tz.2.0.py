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
                , WORKFLOW_NAME='PP_CSNC450B',WORKFLOW_ID='a4d8befb714e4cfdb55940368db306bc', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNC450B-0.0.prd-tz.2.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 10, 45, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('a4d8befb714e4cfdb55940368db306bc')

    csnc450bJob_vol = []
    csnc450bJob_volMnt = []
    csnc450bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csnc450bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csnc450bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csnc450bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csnc450bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnc450bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '663eb5701586459891e7249552c6c277',
        'volumes': csnc450bJob_vol,
        'volume_mounts': csnc450bJob_volMnt,
        'env_from':csnc450bJob_env,
        'task_id':'csnc450bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=csnc450bJob","empNo=82265795", "gubun=3", "date="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('a4d8befb714e4cfdb55940368db306bc')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnc450bJob,
        Complete
    ]) 

    # authCheck >> csnc450bJob >> Complete
    workflow








